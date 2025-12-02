import streamlit as st
import requests
import pandas as pd
import pulp
import time
from datetime import datetime, date
import math
import sqlite3
import json

# --- OPTIONAL: FIREBASE ADMIN FOR CLOUD LOGGING ---
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False

# --- CONFIGURATION ---
BASE_URL = "https://nbafantasy.nba.com/api"
DEFAULT_TEAM_ID = 17
DEFAULT_GAMEWEEK = 7
ADMIN_PASSWORD = "admin124"  # Change this for security

POSITIONS = {"Back Court": 5, "Front Court": 5}
MAX_PLAYERS_PER_TEAM = 2
TRANSFERS_ALLOWED = 2
ROSTER_SIZE = 10

st.set_page_config(page_title="NBA Fantasy Optimizer", layout="wide", page_icon="🏀")

# --- DATABASE & LOGGING FUNCTIONS ---

def get_firestore_db():
    """Initializes and returns Firestore DB client if secrets are available."""
    if not FIREBASE_AVAILABLE:
        return None
    
    # Check if firebase config is in Streamlit secrets
    if "firebase" not in st.secrets:
        return None

    try:
        # Check if app is already initialized to avoid error on rerun
        if not firebase_admin._apps:
            # Create a credential object from the secrets dictionary
            cred_dict = dict(st.secrets["firebase"])
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        
        return firestore.client()
    except Exception as e:
        st.warning(f"Firebase init failed: {e}")
        return None

def init_local_db():
    """Initializes the SQLite database for local fallback."""
    conn = sqlite3.connect('nba_fantasy_logs.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            team_id INTEGER,
            gameweek INTEGER,
            weeks_planned INTEGER,
            status TEXT,
            duration_sec REAL,
            error_msg TEXT,
            result_summary TEXT
        )
    ''')
    conn.commit()
    conn.close()

def log_simulation_start(team_id, gw, weeks):
    """Logs start to Firestore (if avail) or SQLite."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db = get_firestore_db()
    
    if db:
        # --- CLOUD LOGGING (FIRESTORE) ---
        doc_ref = db.collection("logs").document()
        doc_ref.set({
            "timestamp": ts,
            "team_id": team_id,
            "gameweek": gw,
            "weeks_planned": weeks,
            "status": "STARTED",
            "created_at": firestore.SERVER_TIMESTAMP
        })
        return doc_ref.id  # Return Document ID string
    else:
        # --- LOCAL LOGGING (SQLITE) ---
        init_local_db()
        conn = sqlite3.connect('nba_fantasy_logs.db')
        c = conn.cursor()
        c.execute(
            "INSERT INTO logs (timestamp, team_id, gameweek, weeks_planned, status) VALUES (?, ?, ?, ?, ?)",
            (ts, team_id, gw, weeks, 'STARTED')
        )
        log_id = c.lastrowid
        conn.commit()
        conn.close()
        return log_id

def log_simulation_end(log_id, status, duration, error_msg=None, result_summary=None):
    """Updates the log entry with results."""
    db = get_firestore_db()
    
    if db:
        # --- CLOUD UPDATE ---
        if log_id:
            db.collection("logs").document(str(log_id)).update({
                "status": status,
                "duration_sec": duration,
                "error_msg": error_msg if error_msg else "",
                "result_summary": result_summary if result_summary else ""
            })
    else:
        # --- LOCAL UPDATE ---
        conn = sqlite3.connect('nba_fantasy_logs.db')
        c = conn.cursor()
        c.execute(
            "UPDATE logs SET status=?, duration_sec=?, error_msg=?, result_summary=? WHERE id=?",
            (status, duration, error_msg, result_summary, log_id)
        )
        conn.commit()
        conn.close()

def get_all_logs():
    """Fetches logs from the active source."""
    db = get_firestore_db()
    
    if db:
        # --- FETCH FROM CLOUD ---
        try:
            docs = db.collection("logs").order_by("created_at", direction=firestore.Query.DESCENDING).stream()
            data = []
            for doc in docs:
                d = doc.to_dict()
                # Flatten for dataframe
                d['id'] = doc.id
                data.append(d)
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"Error fetching Cloud logs: {e}")
            return pd.DataFrame()
    else:
        # --- FETCH FROM LOCAL ---
        init_local_db()
        conn = sqlite3.connect('nba_fantasy_logs.db')
        df = pd.read_sql_query("SELECT * FROM logs ORDER BY id DESC", conn)
        conn.close()
        return df

# --- HELPER FUNCTIONS ---

def fetch_json(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        return None

@st.cache_data(ttl=3600)
def fetch_bootstrap():
    return fetch_json(f"{BASE_URL}/bootstrap-static/")

@st.cache_data(ttl=3600)
def fetch_fixtures():
    return fetch_json(f"{BASE_URL}/fixtures/")

def get_gameweek_event_range(bootstrap, gameweek):
    """Returns list of event IDs for a specific gameweek."""
    phases = bootstrap.get('phases', [])
    target_phase = None
    gw_name = f"Gameweek {gameweek}"
    
    for phase in phases:
        if phase['name'] == gw_name:
            target_phase = phase
            break
            
    if not target_phase: return []
    return list(range(target_phase['start_event'], target_phase['stop_event'] + 1))

def fetch_picks(team_id, event_id):
    """Fetches picks/stats for a specific completed or active event."""
    url = f"{BASE_URL}/entry/{team_id}/event/{event_id}/picks/"
    return fetch_json(url)

def calculate_selling_price(purchase_price, now_cost):
    if now_cost <= purchase_price: return now_cost
    profit = now_cost - purchase_price
    fee = math.ceil(profit / 2)
    return now_cost - fee

@st.cache_data(ttl=86400)
def get_player_history_avg(player_id):
    """Calculates avg points for last 5 active games."""
    url = f"{BASE_URL}/element-summary/{player_id}/"
    data = fetch_json(url)
    if not data: return 0.0
    
    history = data.get('history', [])
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    history = [h for h in history if h['kickoff_time'][:10] < today_str]
    history.sort(key=lambda x: x['kickoff_time'], reverse=True)
    
    if len(history) >= 2:
        m1 = int(history[0].get('minutes', 0))
        m2 = int(history[1].get('minutes', 0))
        if m1 == 0 and m2 == 0: return None

    played_games = [g for g in history if int(g.get('minutes', 0)) > 0]
    last_5 = played_games[:5]
    
    if not last_5: return 0.0
    total_points = sum(g['total_points'] for g in last_5)
    return total_points / len(last_5)

def get_player_score_for_date(player_id, target_date):
    """Finds the points scored by a player on a specific date string."""
    url = f"{BASE_URL}/element-summary/{player_id}/"
    data = fetch_json(url)
    if not data: return 0
    history = data.get('history', [])
    for h in history:
        if h['kickoff_time'].startswith(target_date):
            return h['total_points']
    return 0

# --- MAIN APP UI ---

st.title("🏀 NBA Fantasy Optimizer (Live)")
st.markdown("Optimize lineup and transfers accounting for **Mid-Week Progress** across multiple weeks.")

# Sidebar
with st.sidebar:
    st.header("Settings")
    team_id_input = st.number_input("Team ID", value=DEFAULT_TEAM_ID, step=1)
    gameweek_input = st.number_input("Start Gameweek", value=DEFAULT_GAMEWEEK, step=1)
    weeks_to_optimize = st.selectbox("Weeks to Plan Ahead", [1, 2, 3], index=0)
    safety_margin = st.number_input("Budget Safety Margin (0.1m units)", value=1, min_value=0)
    
    st.markdown("---")
    st.caption("Simulation Mode")
    use_sim_mode = st.checkbox("Simulate specific Game Day?")
    sim_game_day = st.number_input("Current Game Day of start Gameweek (1-7)", min_value=1, max_value=7, value=1, disabled=not use_sim_mode)
    
    st.markdown("---")
    run_btn = st.button("RUN OPTIMIZATION", type="primary")
    
    # --- ADMIN PANEL (HIDDEN) ---
    # Only visible if URL contains ?admin=true
    if st.query_params.get("admin") == "true":
        st.markdown("---")
        with st.expander("Admin Access"):
            password = st.text_input("Password", type="password")
            if password == ADMIN_PASSWORD:
                st.success("Access Granted")
                if st.button("Refresh Logs"):
                    st.rerun()
                
                if get_firestore_db():
                    st.caption("Source: Google Cloud Firestore (Persistent)")
                else:
                    st.caption("Source: Local SQLite (Ephemeral)")

                logs_df = get_all_logs()
                st.dataframe(logs_df, hide_index=True)
                
                csv = logs_df.to_csv(index=False)
                st.download_button("Download Logs CSV", csv, "nba_optimizer_logs.csv", "text/csv")
            elif password:
                st.error("Incorrect Password")

if run_btn:
    # --- START LOGGING ---
    start_time = time.time()
    log_id = log_simulation_start(team_id_input, gameweek_input, weeks_to_optimize)
    
    try:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 1. Fetch Data
        status_text.text("Fetching game data...")
        bootstrap = fetch_bootstrap()
        if not bootstrap:
            raise Exception("Failed to fetch bootstrap data")
        
        elements = pd.DataFrame(bootstrap['elements'])
        teams = pd.DataFrame(bootstrap['teams'])
        element_types = pd.DataFrame(bootstrap['element_types'])
        
        team_map = pd.Series(teams.name.values, index=teams.id).to_dict()
        if 'short_name' in teams.columns:
            team_short_map = pd.Series(teams.short_name.values, index=teams.id).to_dict()
        else:
            team_short_map = pd.Series(teams.name.str[:3].str.upper().values, index=teams.id).to_dict()
        pos_map = pd.Series(element_types.singular_name.values, index=element_types.id).to_dict()
        
        elements['team_name'] = elements['team'].map(team_map)
        elements['team_short'] = elements['team'].map(team_short_map)
        elements['position_name'] = elements['element_type'].map(pos_map)
        elements['full_name'] = elements['first_name'] + " " + elements['second_name']
        
        active_players = elements[elements['status'] != 'u'].copy()
        
        # 2. Determine Schedule
        status_text.text(f"Building schedule for {weeks_to_optimize} weeks...")
        weeks_schedule = []
        all_target_event_ids = []
        event_to_gw_map = {}
        
        for w in range(weeks_to_optimize):
            current_gw = gameweek_input + w
            ev_range = get_gameweek_event_range(bootstrap, current_gw)
            ev_range.sort()
            if not ev_range:
                st.warning(f"Could not find schedule for Gameweek {current_gw}. Stopping at week {w}.")
                break
            weeks_schedule.append({'gw': current_gw, 'events': ev_range})
            all_target_event_ids.extend(ev_range)
            for eid in ev_range: event_to_gw_map[eid] = current_gw
        
        if not all_target_event_ids:
            raise Exception("No valid events found in schedule")

        fixtures_data = fetch_fixtures()
        fixtures = pd.DataFrame(fixtures_data)
        gw_fixtures = fixtures[fixtures['event'].isin(all_target_event_ids)].copy()
        
        event_dates = {}
        for eid in all_target_event_ids:
            f = gw_fixtures[gw_fixtures['event'] == eid]
            if not f.empty: event_dates[eid] = f.iloc[0]['kickoff_time'][:10]
            else: event_dates[eid] = "Unknown"

        # 3. Identify Current State
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        week1_events = weeks_schedule[0]['events']
        
        if use_sim_mode:
            split_idx = sim_game_day - 1
            if split_idx >= len(week1_events): split_idx = len(week1_events)
            past_event_ids = week1_events[:split_idx]
            future_event_ids = week1_events[split_idx:]
            for w_data in weeks_schedule[1:]: future_event_ids.extend(w_data['events'])
            st.info(f"Simulating from Game Day {sim_game_day} of Gameweek {gameweek_input}.")
        else:
            past_event_ids = [eid for eid in all_target_event_ids if event_dates.get(eid, "9999") < today_str]
            future_event_ids = [eid for eid in all_target_event_ids if event_dates.get(eid, "0000") >= today_str]
        
        if not future_event_ids:
            raise Exception("All selected gameweeks have concluded")

        event_id_to_solver_idx = {eid: i for i, eid in enumerate(future_event_ids)}
        roster_source_event_id = past_event_ids[-1] if past_event_ids else week1_events[0] - 1

        # 4. Analyze History
        banked_points = 0.0
        transfers_used_w1 = 0
        past_day_stats = {} 
        captain_used_map = {w['gw']: False for w in weeks_schedule}
        
        if past_event_ids:
            status_text.text("Calculating banked points...")
            for eid in past_event_ids:
                data = fetch_picks(team_id_input, eid)
                if data:
                    raw_pts = data['entry_history']['points']
                    daily_pts = raw_pts / 10.0
                    banked_points += daily_pts
                    
                    if eid in week1_events:
                        transfers_used_w1 += data['entry_history']['event_transfers']
                    
                    gw_num = event_to_gw_map.get(eid)
                    if gw_num:
                        for p in data['picks']:
                            if p['is_captain'] and p['multiplier'] > 1:
                                captain_used_map[gw_num] = True
                                break
                    
                    past_day_stats[eid] = {'score': daily_pts, 'picks': data['picks']}
        
        transfers_limit_map = {}
        for i, w_data in enumerate(weeks_schedule):
            gw_num = w_data['gw']
            limit = max(0, TRANSFERS_ALLOWED - transfers_used_w1) if i == 0 else TRANSFERS_ALLOWED
            transfers_limit_map[gw_num] = limit

        # 5. Fetch Initial Roster
        status_text.text("Fetching initial roster...")
        my_team_data = fetch_picks(team_id_input, roster_source_event_id)
        if not my_team_data and not past_event_ids:
            my_team_data = fetch_picks(team_id_input, roster_source_event_id - 1)
        if not my_team_data:
            raise Exception(f"Could not fetch initial roster for team {team_id_input}")
            
        picks = my_team_data['picks']
        my_bank = my_team_data['entry_history']['bank']
        
        my_player_ids = [p['element'] for p in picks]
        my_selling_prices = {}
        current_roster_liquidation_value = 0
        
        for p in picks:
            pid = p['element']
            now_cost = active_players.loc[active_players['id'] == pid, 'now_cost'].values[0]
            purchase_price = p.get('purchase_price', now_cost)
            sell_price = calculate_selling_price(purchase_price, now_cost)
            my_selling_prices[pid] = sell_price
            current_roster_liquidation_value += sell_price
            
        total_budget_safe = current_roster_liquidation_value + my_bank - safety_margin
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Banked Points (GW1)", f"{banked_points:.1f}")
        c2.metric("Transfers Left (GW1)", transfers_limit_map[weeks_schedule[0]['gw']])
        c3.metric("Budget", f"{total_budget_safe/10}m")
        c4.metric("Optimize Days", len(future_event_ids))

        # 6. Stats
        status_text.text("Calculating player stats...")
        available_players = active_players[
            (active_players['chance_of_playing_next_round'].isnull()) | 
            (active_players['chance_of_playing_next_round'] > 50)
        ]
        top_candidates = available_players.sort_values('total_points', ascending=False).head(200)
        candidate_ids = set(top_candidates['id'].tolist())
        for pid in my_player_ids: candidate_ids.add(pid)
        
        players_to_fetch = active_players[active_players['id'].isin(candidate_ids)]
        player_eps = {}
        
        for i, (index, player) in enumerate(players_to_fetch.iterrows()):
            if i % 20 == 0: progress_bar.progress(int((i / len(players_to_fetch)) * 90))
            pid = player['id']
            chance = player['chance_of_playing_next_round']
            is_doubtful = False
            if pd.notna(chance) and chance <= 50: is_doubtful = True
            
            avg = get_player_history_avg(pid)
            if avg is None or is_doubtful:
                if pid in my_player_ids: avg = 0.0
                else: continue
            player_eps[pid] = avg

        # 7. Optimization LOOP
        progress_bar.progress(95)
        
        teams_list = elements['team'].unique()
        bc_players_all = [p for p in elements['id'] if elements.loc[elements['id']==p, 'position_name'].values[0] in ["Guard", "Back Court"]] # Logic handled in data prep below
        
        players_data = []
        for pid, ep in player_eps.items():
            p_row = elements[elements['id'] == pid].iloc[0]
            pos = p_row['position_name']
            simple_pos = "Back Court" if ("Guard" in pos or "Back" in pos) else "Front Court"
            effective_cost = my_selling_prices[pid] if pid in my_selling_prices else p_row['now_cost']
            
            players_data.append({
                'id': pid, 'name': p_row['web_name'], 'team_short': p_row['team_short'],
                'cost': effective_cost, 'current_val': p_row['now_cost'],
                'pos': simple_pos, 'team': p_row['team'], 'ep': ep
            })
            
        player_schedule = {p['id']: set() for p in players_data}
        for _, f in gw_fixtures.iterrows():
            eid = f['event']
            if eid in event_id_to_solver_idx:
                day_idx = event_id_to_solver_idx[eid]
                for p in players_data:
                    if p['team'] == f['team_h'] or p['team'] == f['team_a']:
                        player_schedule[p['id']].add(day_idx)

        num_future_days = len(future_event_ids)
        
        # OPTION GENERATION LOOP
        previous_solutions_constraints = []
        
        option_tabs = st.tabs(["🏆 Option 1 (Best)", "🥈 Option 2", "🥉 Option 3"])
        
        best_total_score = 0 # To store for log
        
        for opt_idx in range(3):
            status_text.text(f"Calculating Option {opt_idx + 1}...")
            
            prob = pulp.LpProblem(f"NBA_Fantasy_Opt_{opt_idx}", pulp.LpMaximize)
            
            # Variables
            roster_vars = {} 
            trans_in_vars = {}
            starter_vars = {} 
            captain_vars = {}
            
            for d_idx in range(num_future_days):
                for p in players_data:
                    pid = p['id']
                    roster_vars[(pid, d_idx)] = pulp.LpVariable(f"R_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
                    trans_in_vars[(pid, d_idx)] = pulp.LpVariable(f"T_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
                    
                    if d_idx in player_schedule[pid]:
                        starter_vars[(pid, d_idx)] = pulp.LpVariable(f"S_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
                        captain_vars[(pid, d_idx)] = pulp.LpVariable(f"C_{pid}_{d_idx}", 0, 1, pulp.LpBinary)

            # Basic Rules
            for p in players_data:
                pid = p['id']
                is_owned = 1 if pid in my_player_ids else 0
                prob += trans_in_vars[(pid, 0)] >= roster_vars[(pid, 0)] - is_owned
                for d_idx in range(1, num_future_days):
                    prob += trans_in_vars[(pid, d_idx)] >= roster_vars[(pid, d_idx)] - roster_vars[(pid, d_idx-1)]

            bc_players = [p for p in players_data if p['pos'] == "Back Court"]
            fc_players = [p for p in players_data if p['pos'] == "Front Court"]
            total_obj = 0
            
            for d_idx in range(num_future_days):
                prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] for p in players_data]) == ROSTER_SIZE
                prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] * p['cost'] for p in players_data]) <= total_budget_safe
                prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] for p in bc_players]) == 5
                prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] for p in fc_players]) == 5
                
                for t in teams_list:
                    t_players = [p for p in players_data if p['team'] == t]
                    prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] for p in t_players]) <= MAX_PLAYERS_PER_TEAM

                day_starters = [starter_vars[(p['id'], d_idx)] for p in players_data if (p['id'], d_idx) in starter_vars]
                if day_starters:
                    prob += pulp.lpSum(day_starters) <= 5
                    day_bc = [starter_vars[(p['id'], d_idx)] for p in bc_players if (p['id'], d_idx) in starter_vars]
                    day_fc = [starter_vars[(p['id'], d_idx)] for p in fc_players if (p['id'], d_idx) in starter_vars]
                    prob += pulp.lpSum(day_bc) <= 3
                    prob += pulp.lpSum(day_fc) <= 3
                    
                for p in players_data:
                    pid = p['id']
                    if (pid, d_idx) in starter_vars:
                        prob += starter_vars[(pid, d_idx)] <= roster_vars[(pid, d_idx)]
                        prob += captain_vars[(pid, d_idx)] <= starter_vars[(pid, d_idx)]
                        total_obj += starter_vars[(pid, d_idx)] * p['ep']
                        total_obj += captain_vars[(pid, d_idx)] * p['ep']

            # Aggregated Constraints (Weekly)
            for w_data in weeks_schedule:
                gw_num = w_data['gw']
                gw_indices = [event_id_to_solver_idx[eid] for eid in w_data['events'] if eid in event_id_to_solver_idx]
                
                if gw_indices:
                    week_transfers = []
                    for d_idx in gw_indices:
                        day_trans_vars = [trans_in_vars[(p['id'], d_idx)] for p in players_data]
                        week_transfers.extend(day_trans_vars)
                    prob += pulp.lpSum(week_transfers) <= transfers_limit_map[gw_num]
                    
                    week_captains = []
                    for d_idx in gw_indices:
                        day_caps = [captain_vars[(p['id'], d_idx)] for p in players_data if (p['id'], d_idx) in captain_vars]
                        week_captains.extend(day_caps)
                    
                    if captain_used_map.get(gw_num, False):
                        prob += pulp.lpSum(week_captains) == 0
                    else:
                        prob += pulp.lpSum(week_captains) == 1

            # APPLY PREVIOUS SOLUTION EXCLUSIONS (Integer Cuts)
            for prev_sol_roster in previous_solutions_constraints:
                prob += pulp.lpSum([roster_vars[(pid, d)] for pid, d in prev_sol_roster]) <= len(prev_sol_roster) - 1

            prob += total_obj
            prob.solve(pulp.PULP_CBC_CMD(msg=0))
            
            if pulp.LpStatus[prob.status] != 'Optimal':
                with option_tabs[opt_idx]:
                    st.warning("No valid strategy found for this option.")
                continue
                
            # Store solution for next iteration
            current_sol_roster = []
            for d_idx in range(num_future_days):
                for p in players_data:
                    if roster_vars[(p['id'], d_idx)].varValue > 0.5:
                        current_sol_roster.append((p['id'], d_idx))
            previous_solutions_constraints.append(current_sol_roster)
            
            # RENDER RESULTS FOR THIS OPTION
            future_proj = pulp.value(prob.objective) / 10
            total_proj = banked_points + future_proj
            
            if opt_idx == 0: best_total_score = total_proj # Log best score
            
            with option_tabs[opt_idx]:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total Projected", f"{total_proj:.1f}")
                c2.metric("Future Points", f"{future_proj:.1f}")
                c3.metric("Banked Points", f"{banked_points:.1f}")
                
                previous_roster_ids = set(my_player_ids)
                
                for w_data in weeks_schedule:
                    gw_num = w_data['gw']
                    gw_events = w_data['events']
                    if not any(e in past_event_ids or e in future_event_ids for e in gw_events): continue
                    
                    with st.expander(f"Gameweek {gw_num}", expanded=(gw_num == gameweek_input)):
                        day_tabs = st.tabs([f"Day {i+1}" for i in range(len(gw_events))])
                        
                        for i, eid in enumerate(gw_events):
                            with day_tabs[i]:
                                if eid in past_event_ids:
                                    date_label = event_dates.get(eid, '?')
                                    st.caption(f"Status: COMPLETED | Date: {date_label}")
                                    if eid in past_day_stats:
                                        stats = past_day_stats[eid]
                                        st.metric("Score", f"{stats['score']:.1f}")
                                        r_list = []
                                        for pick in stats['picks']:
                                            pid = pick['element']
                                            p_row = active_players.loc[active_players['id'] == pid]
                                            name = p_row['web_name'].values[0] if not p_row.empty else "Unknown"
                                            role = "Starter"
                                            if pick['multiplier'] == 0: role = "Bench"
                                            if pick['is_captain'] and pick['multiplier'] > 1: role = "CAPTAIN ⭐"
                                            
                                            # Actual Points
                                            actual_pts = get_player_score_for_date(pid, date_label)
                                            r_list.append({"Name": name, "Role": role, "Score": f"{actual_pts/10:.1f}"})
                                        st.dataframe(pd.DataFrame(r_list), use_container_width=True, hide_index=True)
                                    else: st.info("No data.")
                                
                                elif eid in future_event_ids:
                                    d_idx = event_id_to_solver_idx[eid]
                                    roster_today = []
                                    roster_ids = set()
                                    for p in players_data:
                                        if roster_vars[(p['id'], d_idx)].varValue > 0.5:
                                            roster_today.append(p)
                                            roster_ids.add(p['id'])
                                    
                                    trans_in = roster_ids - previous_roster_ids
                                    trans_out = previous_roster_ids - roster_ids
                                    if trans_in:
                                        st.markdown("**Transfers:**")
                                        for pid in trans_out:
                                            p_obj = next((x for x in players_data if x['id'] == pid), None)
                                            st.error(f"OUT: {p_obj['name']}")
                                        for pid in trans_in:
                                            p_obj = next(x for x in players_data if x['id'] == pid)
                                            st.success(f"IN: {p_obj['name']}")
                                    
                                    previous_roster_ids = roster_ids
                                    
                                    l_data = []
                                    for p in roster_today:
                                        pid = p['id']
                                        status = "Bench"
                                        points = 0.0
                                        if (pid, d_idx) in starter_vars and starter_vars[(pid, d_idx)].varValue > 0.5:
                                            status = "Starter"
                                            points = p['ep'] / 10
                                            if (pid, d_idx) in captain_vars and captain_vars[(pid, d_idx)].varValue > 0.5:
                                                status = "CAPTAIN ⭐"
                                                points *= 2
                                        elif d_idx not in player_schedule[pid]: status = "No Game"
                                        l_data.append({"Name": p['name'], "Team": p['team_short'], "Pos": p['pos'], "Role": status, "Exp Pts": f"{points:.1f}"})
                                    
                                    df = pd.DataFrame(l_data)
                                    role_order = {"CAPTAIN ⭐": 0, "Starter": 1, "Bench": 2, "No Game": 3}
                                    df['sort'] = df['Role'].map(role_order)
                                    st.dataframe(df.sort_values('sort').drop('sort', axis=1), use_container_width=True, hide_index=True)

        # Log Success
        log_simulation_end(log_id, 'SUCCESS', time.time()-start_time, result_summary=f"Score: {best_total_score:.1f}")
        
    except Exception as e:
        log_simulation_end(log_id, 'ERROR', time.time()-start_time, error_msg=str(e))
        st.error(f"An error occurred: {str(e)}")