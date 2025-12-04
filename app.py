import streamlit as st
import requests
import pandas as pd
import pulp
import time
from datetime import datetime, timedelta
import math
import sqlite3
import json
import socket

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

POSITIONS = {"Back Court": 5, "Front Court": 5}
MAX_PLAYERS_PER_TEAM = 2
TRANSFERS_ALLOWED = 2
ROSTER_SIZE = 10

st.set_page_config(page_title="NBA Fantasy Optimizer", layout="wide", page_icon="🏀")

# --- HIDE STREAMLIT BRANDING & GITHUB LINKS ---
st.markdown("""
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .stDeployButton {display:none;}
        [data-testid="stToolbar"] {visibility: hidden !important;}
    </style>
""", unsafe_allow_html=True)

# --- DATABASE & LOGGING FUNCTIONS ---

def get_firestore_db():
    if not FIREBASE_AVAILABLE: return None
    if "firebase" not in st.secrets: return None
    try:
        if not firebase_admin._apps:
            cred_dict = dict(st.secrets["firebase"])
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        return None

def get_remote_ip():
    try:
        if hasattr(st, "context") and hasattr(st.context, "headers"):
            headers = st.context.headers
            if "X-Forwarded-For" in headers:
                return headers["X-Forwarded-For"].split(",")[0]
    except Exception: pass
    return "Unknown/Local"

def get_ip_location(ip):
    if ip in ["Unknown/Local", "127.0.0.1", "localhost", "::1"]: return "Localhost"
    try:
        response = requests.get(f"http://ip-api.com/json/{ip}", timeout=2)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                return f"{data.get('city')}, {data.get('regionName')}, {data.get('country')}"
    except: pass
    return "Unknown Location"

def get_pst_time():
    utc = datetime.utcnow()
    pst = utc - timedelta(hours=8)
    return pst.strftime("%Y-%m-%d %H:%M:%S PST")

def init_local_db():
    conn = sqlite3.connect('nba_fantasy_logs.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            ip_address TEXT,
            location TEXT,
            team_id INTEGER,
            gameweek INTEGER,
            weeks_planned INTEGER,
            user_options TEXT,
            status TEXT,
            duration_sec REAL,
            error_msg TEXT,
            result_summary TEXT,
            transfers TEXT
        )
    ''')
    c.execute("PRAGMA table_info(logs)")
    cols = [info[1] for info in c.fetchall()]
    if 'ip_address' not in cols: c.execute("ALTER TABLE logs ADD COLUMN ip_address TEXT")
    if 'location' not in cols: c.execute("ALTER TABLE logs ADD COLUMN location TEXT")
    if 'transfers' not in cols: c.execute("ALTER TABLE logs ADD COLUMN transfers TEXT")
    if 'user_options' not in cols: c.execute("ALTER TABLE logs ADD COLUMN user_options TEXT")
    conn.commit()
    conn.close()

def log_simulation_start(team_id, gw, weeks, options_dict):
    ts = get_pst_time()
    ip = get_remote_ip()
    loc = get_ip_location(ip)
    options_str = json.dumps(options_dict)
    
    db = get_firestore_db()
    if db:
        doc_ref = db.collection("logs").document()
        doc_ref.set({
            "timestamp": ts, "ip_address": ip, "location": loc,
            "team_id": team_id, "gameweek": gw, "weeks_planned": weeks,
            "user_options": options_str,
            "status": "STARTED", "created_at": firestore.SERVER_TIMESTAMP
        })
        return doc_ref.id
    else:
        init_local_db()
        conn = sqlite3.connect('nba_fantasy_logs.db')
        c = conn.cursor()
        c.execute("INSERT INTO logs (timestamp, ip_address, location, team_id, gameweek, weeks_planned, user_options, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ts, ip, loc, team_id, gw, weeks, options_str, 'STARTED'))
        log_id = c.lastrowid
        conn.commit()
        conn.close()
        return log_id

def log_simulation_end(log_id, status, duration, error_msg=None, result_summary=None, transfers=None):
    db = get_firestore_db()
    if db:
        if log_id:
            db.collection("logs").document(str(log_id)).update({
                "status": status, "duration_sec": duration,
                "error_msg": error_msg if error_msg else "",
                "result_summary": result_summary if result_summary else "",
                "transfers": transfers if transfers else ""
            })
    else:
        conn = sqlite3.connect('nba_fantasy_logs.db')
        c = conn.cursor()
        c.execute("UPDATE logs SET status=?, duration_sec=?, error_msg=?, result_summary=?, transfers=? WHERE id=?",
            (status, duration, error_msg, result_summary, transfers, log_id))
        conn.commit()
        conn.close()

def get_all_logs():
    db = get_firestore_db()
    if db:
        try:
            docs = db.collection("logs").order_by("created_at", direction=firestore.Query.DESCENDING).stream()
            data = []
            for doc in docs:
                d = doc.to_dict()
                d['id'] = doc.id
                if 'created_at' in d and d['created_at']:
                    if 'timestamp' not in d: d['timestamp'] = d['created_at'].strftime("%Y-%m-%d %H:%M:%S")
                data.append(d)
            return pd.DataFrame(data)
        except Exception: return pd.DataFrame()
    else:
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
    url = f"{BASE_URL}/entry/{team_id}/event/{event_id}/picks/"
    return fetch_json(url)

def calculate_selling_price(purchase_price, now_cost):
    if now_cost <= purchase_price: return now_cost
    profit = now_cost - purchase_price
    fee = math.ceil(profit / 2)
    return now_cost - fee

@st.cache_data(ttl=86400)
def get_player_history_avg(player_id):
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
    played_games = [g for g in history if g.get('total_points', 0) > 0]
    last_5 = played_games[:5]
    if not last_5: return 0.0
    total_points = sum(g['total_points'] for g in last_5)
    return total_points / len(last_5)

def get_player_score_for_date(player_id, target_date):
    url = f"{BASE_URL}/element-summary/{player_id}/"
    data = fetch_json(url)
    if not data: return 0
    history = data.get('history', [])
    for h in history:
        if h['kickoff_time'].startswith(target_date):
            return h['total_points']
    return 0

# --- NBA CUP PROBABILITY HELPERS ---
def get_win_probability(team1_id, team2_id, teams_df):
    try:
        t1 = teams_df[teams_df['id'] == team1_id].iloc[0]
        t2 = teams_df[teams_df['id'] == team2_id].iloc[0]
        w1, l1 = t1.get('win', 0), t1.get('loss', 0)
        w2, l2 = t2.get('win', 0), t2.get('loss', 0)
        total1 = w1 + l1
        rate1 = w1 / total1 if total1 > 0 else 0.5
        total2 = w2 + l2
        rate2 = w2 / total2 if total2 > 0 else 0.5
        if rate1 + rate2 == 0: return 0.5
        return rate1 / (rate1 + rate2)
    except: return 0.5

# --- ADMIN PAGE ---
if st.query_params.get("admin") == "true":
    st.title("🔒 NBA Fantasy Optimizer - Admin Panel")
    
    if "admin_password" not in st.secrets:
        st.error("Admin password not configured in secrets. Access Denied.")
        st.stop()
        
    password = st.text_input("Enter Admin Password", type="password")
    
    if password == st.secrets["admin_password"]:
        st.success("Access Granted")
        if st.button("Refresh Logs"): st.rerun()
        if get_firestore_db(): st.caption("Source: Cloud")
        else: st.caption("Source: Local")
        logs_df = get_all_logs()
        if not logs_df.empty:
            logs_df['date_group'] = logs_df['timestamp'].apply(lambda x: str(x)[:10] if x else "Unknown")
            unique_dates = sorted(logs_df['date_group'].unique(), reverse=True)
            st.markdown("### Log History")
            for d in unique_dates:
                day_logs = logs_df[logs_df['date_group'] == d]
                count = len(day_logs)
                with st.expander(f"📅 {d} ({count} logs)", expanded=(d == unique_dates[0])):
                    st.dataframe(day_logs, use_container_width=True, hide_index=True)
            st.markdown("---")
            csv = logs_df.to_csv(index=False)
            st.download_button("Download All Logs CSV", csv, "nba_optimizer_logs.csv", "text/csv")
        else: st.info("No logs found.")
    elif password: st.error("Incorrect Password")
    st.stop()

# --- MAIN APP UI ---

st.title("🏀 NBA Fantasy Optimizer (Live)")
st.markdown("Optimize lineup and transfers accounting for **Mid-Week Progress** across multiple weeks.")

# 1. PRE-FETCH STATIC DATA
bootstrap = fetch_bootstrap()
if not bootstrap:
    st.error("Failed to fetch NBA Fantasy data. Please try again later.")
    st.stop()

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

# SIDEBAR
with st.sidebar:
    st.header("Settings")
    team_id_input = st.number_input("Team ID", value=DEFAULT_TEAM_ID, step=1)
    gameweek_input = st.number_input("Start Gameweek", value=DEFAULT_GAMEWEEK, step=1)
    weeks_to_optimize = st.selectbox("Weeks to Plan Ahead", [1, 2, 3], index=0)
    safety_margin = st.number_input("Budget Safety Margin (0.1m units)", value=1, min_value=0)
    
    st.markdown("---")
    st.caption("Simulation Mode")
    
    default_sim = False
    default_day = 1
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    gw_events = get_gameweek_event_range(bootstrap, gameweek_input)
    
    if gw_events:
        gw_events.sort()
        fixtures_data = fetch_fixtures()
        if fixtures_data:
            fx = pd.DataFrame(fixtures_data)
            subset = fx[fx['event'].isin(gw_events)]
            for i, eid in enumerate(gw_events):
                ev_games = subset[subset['event'] == eid]
                if not ev_games.empty:
                    e_date = ev_games.iloc[0]['kickoff_time'][:10]
                    if e_date == today_str:
                        day_num = i + 1
                        if day_num >= 2:
                            default_sim = True
                            default_day = day_num
                        break

    use_sim_mode = st.checkbox("Simulate specific Game Day?", value=default_sim)
    sim_game_day = st.number_input("Current Game Day of start Gameweek (1-7)", min_value=1, max_value=7, value=default_day, disabled=not use_sim_mode)
    
    st.markdown("---")
    
    # --- ROSTER PRE-CALC FOR SELECTORS ---
    roster_source_eid = None
    if gw_events:
        roster_source_eid = gw_events[0] - 1
        if fixtures_data:
            fixtures = pd.DataFrame(fixtures_data)
            gw_fixtures = fixtures[fixtures['event'].isin(gw_events)].copy()
            event_dates = {}
            for eid in gw_events:
                f = gw_fixtures[gw_fixtures['event'] == eid]
                if not f.empty: event_dates[eid] = f.iloc[0]['kickoff_time'][:10]
                else: event_dates[eid] = "9999"
            
            if use_sim_mode:
                split_idx = sim_game_day - 1
                past_eids = gw_events[:split_idx]
                if past_eids: roster_source_eid = past_eids[-1]
            else:
                past_eids = [eid for eid in gw_events if event_dates.get(eid, "9999") < today_str]
                if past_eids: roster_source_eid = past_eids[-1]

    current_roster_names = {}
    current_roster_ids_set = set()
    if roster_source_eid:
        team_data = fetch_picks(team_id_input, event_id=roster_source_eid)
        if not team_data:
             team_data = fetch_picks(team_id_input, roster_source_eid - 1)
        if team_data:
            for p in team_data['picks']:
                pid = p['element']
                current_roster_ids_set.add(pid)
                p_row = active_players.loc[active_players['id'] == pid]
                if not p_row.empty:
                    name = f"{p_row['web_name'].values[0]} ({p_row['team_short'].values[0]})"
                    current_roster_names[name] = pid
    
    all_available_for_add = {}
    for idx, row in active_players.iterrows():
        if row['id'] not in current_roster_ids_set:
            name_label = f"{row['web_name']} ({row['team_short']}) - {row['now_cost']/10}m"
            all_available_for_add[name_label] = row['id']

    forced_drop_names = st.multiselect(
        "Force Transfer Out (Start of Sim):",
        options=list(current_roster_names.keys()),
        help="Select players you want to guarantee are sold immediately."
    )
    forced_drop_ids = [current_roster_names[n] for n in forced_drop_names]
    
    forced_keep_names = st.multiselect(
        "Force KEEP (Ignore Injury/Low Chance):",
        options=list(current_roster_names.keys()),
        help="Select players you want to keep even if they are flagged as doubtful (<50% chance)."
    )
    forced_keep_ids = [current_roster_names[n] for n in forced_keep_names]
    
    forced_add_names = st.multiselect(
        "Force Transfer In (Start of Sim):",
        options=list(all_available_for_add.keys()),
        help="Select players you want to guarantee are bought immediately."
    )
    forced_add_ids = [all_available_for_add[n] for n in forced_add_names]
    
    exclude_options = [name for name in all_available_for_add.keys() if name not in forced_add_names]
    forced_exclude_names = st.multiselect(
        "Force Exclude (Do Not Buy):",
        options=exclude_options,
        max_selections=3,
        help="Select up to 3 players to strictly exclude from being transferred in."
    )
    forced_exclude_ids = [all_available_for_add[n] for n in forced_exclude_names]

    st.markdown("---")
    run_btn = st.button("RUN OPTIMIZATION", type="primary")

if run_btn:
    start_time = time.time()
    
    # Gather options for logging
    user_opts = {
        "simulation_mode": use_sim_mode,
        "sim_game_day": sim_game_day if use_sim_mode else "Auto",
        "safety_margin": safety_margin,
        "forced_drop": forced_drop_names,
        "forced_keep": forced_keep_names,
        "forced_add": forced_add_names,
        "forced_exclude": forced_exclude_names
    }
    
    log_id = log_simulation_start(team_id_input, gameweek_input, weeks_to_optimize, user_opts)
    best_total_score = 0.0
    best_option_transfers = []
    
    try:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 2. Determine Schedule
        status_text.text(f"Building schedule for {weeks_to_optimize} weeks...")
        weeks_schedule = []
        all_target_event_ids = []
        event_to_gw_map = {}
        
        for w in range(weeks_to_optimize):
            current_gw = gameweek_input + w
            ev_range = get_gameweek_event_range(bootstrap, current_gw)
            ev_range.sort()
            if not ev_range: break
            weeks_schedule.append({'gw': current_gw, 'events': ev_range})
            all_target_event_ids.extend(ev_range)
            for eid in ev_range: event_to_gw_map[eid] = current_gw
        
        if not all_target_event_ids: raise Exception("No valid events found")

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
        
        if not future_event_ids: raise Exception("All selected gameweeks have concluded")

        event_id_to_solver_idx = {eid: i for i, eid in enumerate(future_event_ids)}
        roster_source_event_id = past_event_ids[-1] if past_event_ids else week1_events[0] - 1

        # 4. Analyze History
        banked_points_total = 0.0
        banked_points_by_gw = {}
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
                    banked_points_total += daily_pts
                    
                    gw = event_to_gw_map.get(eid)
                    if gw: banked_points_by_gw[gw] = banked_points_by_gw.get(gw, 0.0) + daily_pts
                    
                    if eid in week1_events:
                        transfers_used_w1 += data['entry_history']['event_transfers']
                    
                    if gw:
                        for p in data['picks']:
                            if p['is_captain'] and p['multiplier'] > 1:
                                captain_used_map[gw] = True
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
        if not my_team_data: raise Exception(f"Could not fetch initial roster for team {team_id_input}")
            
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
        c1.metric("Banked Points (GW1)", f"{banked_points_total:.1f}")
        c2.metric("Transfers Left (GW1)", transfers_limit_map[weeks_schedule[0]['gw']])
        c3.metric("Budget", f"{total_budget_safe/10}m")
        c4.metric("Optimize Days", len(future_event_ids))

        # 6. Stats
        status_text.text("Calculating player stats...")
        available_players = active_players[
            (active_players['chance_of_playing_next_round'].isnull()) | 
            (active_players['chance_of_playing_next_round'] >= 50)
        ]
        top_candidates = available_players.sort_values('total_points', ascending=False).head(200)
        candidate_ids = set(top_candidates['id'].tolist())
        for pid in my_player_ids: candidate_ids.add(pid)
        for pid in forced_add_ids: candidate_ids.add(pid)
        
        # Filter out Force Excluded players from candidate pool
        for pid in forced_exclude_ids:
            if pid in candidate_ids:
                candidate_ids.remove(pid)
        
        players_to_fetch = active_players[active_players['id'].isin(candidate_ids)]
        player_eps = {}
        
        owned_injured_pids = [] 
        
        for i, (index, player) in enumerate(players_to_fetch.iterrows()):
            if i % 20 == 0: progress_bar.progress(int((i / len(players_to_fetch)) * 90))
            pid = player['id']
            chance = player['chance_of_playing_next_round']
            is_doubtful = False
            
            if pd.notna(chance) and chance < 50: 
                is_doubtful = True
                if pid in my_player_ids:
                    owned_injured_pids.append(pid)
            
            if pid in forced_keep_ids or pid in forced_add_ids: is_doubtful = False

            avg = get_player_history_avg(pid)
            if avg is None or is_doubtful:
                if pid in my_player_ids or pid in forced_add_ids: avg = 0.0
                else: continue
            player_eps[pid] = avg

        # 7. Optimization
        progress_bar.progress(95)
        status_text.text("Optimizing strategy...")
        
        prob = pulp.LpProblem("NBA_Fantasy_MultiWeek", pulp.LpMaximize)
        
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
        
        # --- NBA CUP LOGIC ---
        cup_team_map = {}
        target_teams = ["MIA", "ORL", "NYK", "TOR", "PHX", "OKC", "SAS", "LAL"]
        for t_code in target_teams:
            row = teams[teams['short_name'] == t_code]
            if not row.empty: cup_team_map[t_code] = row.iloc[0]['id']
        
        probs = {}
        if len(cup_team_map) == 8:
            p_a = get_win_probability(cup_team_map['MIA'], cup_team_map['ORL'], teams)
            p_b = get_win_probability(cup_team_map['NYK'], cup_team_map['TOR'], teams)
            p_c = get_win_probability(cup_team_map['PHX'], cup_team_map['OKC'], teams)
            p_d = get_win_probability(cup_team_map['SAS'], cup_team_map['LAL'], teams)
            probs['MIA_D6'] = p_a
            probs['ORL_D6'] = 1 - p_a
            probs['NYK_D6'] = p_b
            probs['TOR_D6'] = 1 - p_b
            probs['PHX_D6'] = p_c
            probs['OKC_D6'] = 1 - p_c
            probs['SAS_D6'] = p_d
            probs['LAL_D6'] = 1 - p_d
            for t in target_teams: probs[f"{t}_FINAL"] = probs[f"{t}_D6"] * 0.5

        player_schedule = {p['id']: {} for p in players_data}
        for _, f in gw_fixtures.iterrows():
            eid = f['event']
            if eid in event_id_to_solver_idx:
                day_idx = event_id_to_solver_idx[eid]
                for p in players_data:
                    if p['team'] == f['team_h'] or p['team'] == f['team_a']:
                        player_schedule[p['id']][day_idx] = 1.0
        
        gw8_evs = get_gameweek_event_range(bootstrap, 8)
        gw9_evs = get_gameweek_event_range(bootstrap, 9)
        
        if gw8_evs and len(gw8_evs) >= 6:
            semi_final_eid = gw8_evs[5]
            if semi_final_eid in event_id_to_solver_idx:
                s_idx = event_id_to_solver_idx[semi_final_eid]
                for p in players_data:
                    t_code = p['team_short']
                    if t_code in cup_team_map:
                        prob_key = f"{t_code}_D6"
                        if prob_key in probs: player_schedule[p['id']][s_idx] = probs[prob_key]

        if gw9_evs and len(gw9_evs) >= 1:
            final_eid = gw9_evs[0]
            if final_eid in event_id_to_solver_idx:
                f_idx = event_id_to_solver_idx[final_eid]
                for p in players_data:
                    t_code = p['team_short']
                    if t_code in cup_team_map:
                        prob_key = f"{t_code}_FINAL"
                        if prob_key in probs: player_schedule[p['id']][f_idx] = probs[prob_key]

        num_future_days = len(future_event_ids)
        previous_solutions_constraints = []
        option_tabs = st.tabs(["🏆 Option 1 (Best)", "🥈 Option 2", "🥉 Option 3"])
        
        for opt_idx in range(3):
            status_text.text(f"Calculating Option {opt_idx + 1}...")
            prob = pulp.LpProblem(f"NBA_Fantasy_Opt_{opt_idx}", pulp.LpMaximize)
            roster_vars = {} 
            trans_in_vars = {}
            starter_vars = {} 
            captain_vars = {}
            
            for d_idx in range(num_future_days):
                for p in players_data:
                    pid = p['id']
                    roster_vars[(pid, d_idx)] = pulp.LpVariable(f"R_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
                    trans_in_vars[(pid, d_idx)] = pulp.LpVariable(f"T_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
                    sched_prob = player_schedule[pid].get(d_idx, 0)
                    if sched_prob > 0:
                        starter_vars[(pid, d_idx)] = pulp.LpVariable(f"S_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
                        captain_vars[(pid, d_idx)] = pulp.LpVariable(f"C_{pid}_{d_idx}", 0, 1, pulp.LpBinary)

            for p in players_data:
                pid = p['id']
                is_owned = 1 if pid in my_player_ids else 0
                prob += trans_in_vars[(pid, 0)] >= roster_vars[(pid, 0)] - is_owned
                for d_idx in range(1, num_future_days):
                    prob += trans_in_vars[(pid, d_idx)] >= roster_vars[(pid, d_idx)] - roster_vars[(pid, d_idx-1)]

            for pid in forced_drop_ids:
                for d_idx in range(num_future_days):
                    if (pid, d_idx) in roster_vars: prob += roster_vars[(pid, d_idx)] == 0

            for pid in forced_add_ids:
                if (pid, 0) in roster_vars: prob += roster_vars[(pid, 0)] == 1

            teams_list = elements['team'].unique()
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
                        
                        game_prob = player_schedule[pid].get(d_idx, 0)
                        total_obj += starter_vars[(pid, d_idx)] * p['ep'] * game_prob
                        total_obj += captain_vars[(pid, d_idx)] * p['ep'] * game_prob

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

            for prev_sol_roster in previous_solutions_constraints:
                prob += pulp.lpSum([roster_vars[(pid, d)] for pid, d in prev_sol_roster]) <= len(prev_sol_roster) - 1

            prob += total_obj
            prob.solve(pulp.PULP_CBC_CMD(msg=0))
            
            if pulp.LpStatus[prob.status] != 'Optimal':
                with option_tabs[opt_idx]: st.warning("No valid strategy found.")
                continue
                
            current_sol_roster = []
            for d_idx in range(num_future_days):
                for p in players_data:
                    if roster_vars[(p['id'], d_idx)].varValue > 0.5:
                        current_sol_roster.append((p['id'], d_idx))
            previous_solutions_constraints.append(current_sol_roster)
            
            future_proj = pulp.value(prob.objective) / 10
            total_proj = banked_points_total + future_proj
            if opt_idx == 0: best_total_score = total_proj
            
            gw_breakdown = {}
            for w_data in weeks_schedule:
                gw = w_data['gw']
                gw_total = banked_points_by_gw.get(gw, 0.0)
                for eid in w_data['events']:
                    if eid in event_id_to_solver_idx:
                        d_idx = event_id_to_solver_idx[eid]
                        for p in players_data:
                            if (p['id'], d_idx) in starter_vars and starter_vars[(p['id'], d_idx)].varValue > 0.5:
                                pts = p['ep'] / 10.0
                                if (p['id'], d_idx) in captain_vars and captain_vars[(p['id'], d_idx)].varValue > 0.5:
                                    pts *= 2
                                gw_total += pts
                gw_breakdown[gw] = gw_total

            with option_tabs[opt_idx]:
                cols = st.columns(len(gw_breakdown) + 1)
                cols[0].metric("Total Score (EV)", f"{total_proj:.1f}")
                for i, (gw, score) in enumerate(gw_breakdown.items()):
                    cols[i+1].metric(f"GW{gw} EV", f"{score:.1f}")
                
                previous_roster_ids = set(my_player_ids)
                best_option_transfers = []
                
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
                                            team_short = p_row['team_short'].values[0] if not p_row.empty else "-"
                                            role = "Starter"
                                            if pick['multiplier'] == 0: role = "Bench"
                                            if pick['is_captain'] and pick['multiplier'] > 1: role = "CAPTAIN ⭐"
                                            actual_pts = get_player_score_for_date(pid, date_label)
                                            r_list.append({"Name": name, "Team": team_short, "Role": role, "Score": f"{actual_pts/10:.1f}"})
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
                                        t_out = []
                                        t_in = []
                                        for pid in trans_out:
                                            p_obj = next((x for x in players_data if x['id'] == pid), None)
                                            st.error(f"OUT: {p_obj['name']}")
                                            t_out.append(p_obj['name'])
                                        for pid in trans_in:
                                            p_obj = next(x for x in players_data if x['id'] == pid)
                                            st.success(f"IN: {p_obj['name']}")
                                            t_in.append(p_obj['name'])
                                        
                                        if opt_idx == 0:
                                            day_label = f"GW{gw_num} D{i+1}"
                                            best_option_transfers.append(f"{day_label}: {', '.join(t_out)} -> {', '.join(t_in)}")
                                    
                                    previous_roster_ids = roster_ids
                                    
                                    l_data = []
                                    for p in roster_today:
                                        pid = p['id']
                                        status = "Bench"
                                        points = 0.0
                                        
                                        game_prob = player_schedule[p['id']].get(d_idx, 0)
                                        
                                        if (pid, d_idx) in starter_vars and starter_vars[(pid, d_idx)].varValue > 0.5:
                                            status = "Starter"
                                            points = (p['ep'] / 10.0) * game_prob
                                            if (pid, d_idx) in captain_vars and captain_vars[(pid, d_idx)].varValue > 0.5:
                                                status = "CAPTAIN ⭐"
                                                points *= 2
                                        elif game_prob == 0: status = "No Game"
                                        
                                        note = ""
                                        if 0 < game_prob < 1:
                                            note = f" ({int(game_prob*100)}% chance)"
                                            
                                        l_data.append({
                                            "Name": p['name'], "Team": p['team_short'],
                                            "Pos": p['pos'], "Value": f"{p['current_val']/10}m",
                                            "Role": status, "Exp Pts": f"{points:.1f}{note}"
                                        })
                                    
                                    df = pd.DataFrame(l_data)
                                    role_order = {"CAPTAIN ⭐": 0, "Starter": 1, "Bench": 2, "No Game": 3}
                                    df['sort'] = df['Role'].map(role_order)
                                    st.dataframe(df.sort_values('sort').drop('sort', axis=1), use_container_width=True, hide_index=True)

        transfers_str = "; ".join(best_option_transfers)
        log_simulation_end(log_id, 'SUCCESS', time.time()-start_time, result_summary=f"Score: {best_total_score:.1f}", transfers=transfers_str)
        
    except Exception as e:
        log_simulation_end(log_id, 'ERROR', time.time()-start_time, error_msg=str(e))
        st.error(f"An error occurred: {str(e)}")