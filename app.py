import streamlit as st
import requests
import pandas as pd
import pulp
import time
from datetime import datetime, date
import math

# --- CONFIGURATION ---
BASE_URL = "https://nbafantasy.nba.com/api"
DEFAULT_TEAM_ID = 17
DEFAULT_GAMEWEEK = 7

POSITIONS = {"Back Court": 5, "Front Court": 5}
MAX_PLAYERS_PER_TEAM = 2
TRANSFERS_ALLOWED = 2
ROSTER_SIZE = 10

st.set_page_config(page_title="NBA Fantasy Optimizer", layout="wide", page_icon="🏀")

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
    """
    Calculates avg points for last 5 active games.
    Active means total_points > 0.
    """
    url = f"{BASE_URL}/element-summary/{player_id}/"
    data = fetch_json(url)
    if not data: return 0.0
    
    history = data.get('history', [])
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    history = [h for h in history if h['kickoff_time'][:10] < today_str]
    history.sort(key=lambda x: x['kickoff_time'], reverse=True)
    
    # Injury Check: Still assume injured if 0 mins in last 2 *recorded* games
    if len(history) >= 2:
        m1 = int(history[0].get('minutes', 0))
        m2 = int(history[1].get('minutes', 0))
        if m1 == 0 and m2 == 0: return None

    # Filter: Active = Points > 0
    played_games = [g for g in history if g.get('total_points', 0) > 0]
    
    # Take last 5
    last_5 = played_games[:5]
    
    if not last_5: return 0.0
    total_points = sum(g['total_points'] for g in last_5)
    return total_points / len(last_5)

# --- MAIN APP UI ---

st.title("🏀 NBA Fantasy Optimizer (Live)")
st.markdown("Optimize lineup and transfers accounting for **Mid-Week Progress** across multiple weeks.")

# Sidebar
with st.sidebar:
    st.header("Settings")
    team_id_input = st.number_input("Team ID", value=DEFAULT_TEAM_ID, step=1)
    
    # Starting Gameweek
    gameweek_input = st.number_input("Start Gameweek", value=DEFAULT_GAMEWEEK, step=1)
    
    # Multi-Week Selection
    weeks_to_optimize = st.selectbox("Weeks to Plan Ahead", [1, 2, 3], index=0, help="Simulate transfers and lineups for up to 3 weeks.")
    
    safety_margin = st.number_input("Budget Safety Margin (0.1m units)", value=1, min_value=0)
    
    st.markdown("---")
    st.caption("Simulation Mode")
    use_sim_mode = st.checkbox("Simulate specific Game Day?")
    sim_game_day = st.number_input("Current Game Day of Week 1 (1-7)", min_value=1, max_value=7, value=1, disabled=not use_sim_mode)
    
    st.markdown("---")
    run_btn = st.button("RUN OPTIMIZATION", type="primary")

if run_btn:
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 1. Fetch Data
    status_text.text("Fetching game data...")
    bootstrap = fetch_bootstrap()
    if not bootstrap: st.stop()
    
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
    
    # 2. Determine Schedule (Multi-Week)
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
        weeks_schedule.append({
            'gw': current_gw,
            'events': ev_range
        })
        all_target_event_ids.extend(ev_range)
        for eid in ev_range:
            event_to_gw_map[eid] = current_gw
    
    if not all_target_event_ids:
        st.error("No valid events found.")
        st.stop()

    fixtures_data = fetch_fixtures()
    fixtures = pd.DataFrame(fixtures_data)
    gw_fixtures = fixtures[fixtures['event'].isin(all_target_event_ids)].copy()
    
    event_dates = {}
    for eid in all_target_event_ids:
        f = gw_fixtures[gw_fixtures['event'] == eid]
        if not f.empty:
            event_dates[eid] = f.iloc[0]['kickoff_time'][:10]
        else:
            event_dates[eid] = "Unknown"

    # 3. Identify Current State
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    week1_events = weeks_schedule[0]['events']
    
    if use_sim_mode:
        split_idx = sim_game_day - 1
        if split_idx >= len(week1_events): split_idx = len(week1_events)
        
        past_event_ids = week1_events[:split_idx]
        future_event_ids = week1_events[split_idx:]
        for w_data in weeks_schedule[1:]:
            future_event_ids.extend(w_data['events'])
            
        st.info(f"Simulating from Game Day {sim_game_day} of Gameweek {gameweek_input}.")
    else:
        past_event_ids = [eid for eid in all_target_event_ids if event_dates.get(eid, "9999") < today_str]
        future_event_ids = [eid for eid in all_target_event_ids if event_dates.get(eid, "0000") >= today_str]
    
    if not future_event_ids:
        st.warning("All selected gameweeks have concluded.")
        st.stop()

    # Map each future event ID to a 0-based index for the solver variables
    event_id_to_solver_idx = {eid: i for i, eid in enumerate(future_event_ids)}

    if past_event_ids:
        roster_source_event_id = past_event_ids[-1]
    else:
        roster_source_event_id = week1_events[0] - 1

    # 4. Analyze History
    banked_points = 0.0
    transfers_used_w1 = 0
    past_day_stats = {} 
    captain_used_map = {w['gw']: False for w in weeks_schedule}
    
    if past_event_ids:
        status_text.text("Calculating banked points & checking history...")
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
                        # Only mark captain used if multiplier > 1
                        if p['is_captain'] and p['multiplier'] > 1:
                            captain_used_map[gw_num] = True
                            break
                
                past_day_stats[eid] = {'score': daily_pts, 'picks': data['picks']}
    
    transfers_limit_map = {}
    for i, w_data in enumerate(weeks_schedule):
        gw_num = w_data['gw']
        if i == 0:
            limit = max(0, TRANSFERS_ALLOWED - transfers_used_w1)
        else:
            limit = TRANSFERS_ALLOWED
        transfers_limit_map[gw_num] = limit

    # 5. Fetch Initial Roster
    status_text.text(f"Fetching roster from Event {roster_source_event_id}...")
    my_team_data = fetch_picks(team_id_input, roster_source_event_id)
    if not my_team_data and not past_event_ids:
        my_team_data = fetch_picks(team_id_input, roster_source_event_id - 1)
    if not my_team_data:
        st.error("Could not fetch initial roster.")
        st.stop()
        
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
    status_text.text("Calculating player stats (Last 5 Avg)...")
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
    
    player_schedule = {p['id']: set() for p in players_data}
    
    for _, f in gw_fixtures.iterrows():
        eid = f['event']
        if eid in event_id_to_solver_idx:
            day_idx = event_id_to_solver_idx[eid]
            for p in players_data:
                if p['team'] == f['team_h'] or p['team'] == f['team_a']:
                    player_schedule[p['id']].add(day_idx)

    num_future_days = len(future_event_ids)
    roster_vars = {} 
    trans_in_vars = {}
    starter_vars = {} 
    captain_vars = {}
    
    for d_idx in range(num_future_days):
        for p in players_data:
            pid = p['id']
            roster_vars[(pid, d_idx)] = pulp.LpVariable(f"Roster_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
            trans_in_vars[(pid, d_idx)] = pulp.LpVariable(f"In_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
            
            if d_idx in player_schedule[pid]:
                starter_vars[(pid, d_idx)] = pulp.LpVariable(f"Start_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
                captain_vars[(pid, d_idx)] = pulp.LpVariable(f"Capt_{pid}_{d_idx}", 0, 1, pulp.LpBinary)

    for p in players_data:
        pid = p['id']
        is_owned = 1 if pid in my_player_ids else 0
        prob += trans_in_vars[(pid, 0)] >= roster_vars[(pid, 0)] - is_owned
        for d_idx in range(1, num_future_days):
            prob += trans_in_vars[(pid, d_idx)] >= roster_vars[(pid, d_idx)] - roster_vars[(pid, d_idx-1)]

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
                total_obj += starter_vars[(pid, d_idx)] * p['ep']
                total_obj += captain_vars[(pid, d_idx)] * p['ep']

    # Aggregated Constraints (Weekly)
    for w_data in weeks_schedule:
        gw_num = w_data['gw']
        gw_events = w_data['events']
        # FIX: Use the unified variable name defined earlier
        gw_indices = [event_id_to_solver_idx[eid] for eid in gw_events if eid in event_id_to_solver_idx]
        
        if gw_indices:
            week_transfers = []
            for d_idx in gw_indices:
                day_trans_vars = [trans_in_vars[(p['id'], d_idx)] for p in players_data]
                week_transfers.extend(day_trans_vars)
            prob += pulp.lpSum(week_transfers) <= transfers_limit_map[gw_num], f"TransLimit_GW{gw_num}"
            
            week_captains = []
            for d_idx in gw_indices:
                day_caps = [captain_vars[(p['id'], d_idx)] for p in players_data if (p['id'], d_idx) in captain_vars]
                week_captains.extend(day_caps)
            
            if captain_used_map.get(gw_num, False):
                prob += pulp.lpSum(week_captains) == 0, f"CaptLimit_GW{gw_num}_Used"
            else:
                prob += pulp.lpSum(week_captains) == 1, f"CaptLimit_GW{gw_num}_New"

    prob += total_obj
    prob.solve(pulp.PULP_CBC_CMD(msg=0))
    
    progress_bar.progress(100)
    status_text.text("Done!")
    
    if pulp.LpStatus[prob.status] != 'Optimal':
        st.error("Optimization failed. Constraints likely too tight.")
    else:
        future_proj = pulp.value(prob.objective) / 10
        total_proj = banked_points + future_proj
        st.success(f"Projected Total (All Weeks): {total_proj:.1f} pts ({banked_points:.1f} Banked + {future_proj:.1f} Future)")
        
        previous_roster_ids = set(my_player_ids)
        
        for w_data in weeks_schedule:
            gw_num = w_data['gw']
            gw_events = w_data['events']
            if not any(e in past_event_ids or e in future_event_ids for e in gw_events): continue
            
            with st.expander(f"Gameweek {gw_num} Details", expanded=True):
                day_tabs = st.tabs([f"Day {i+1}" for i in range(len(gw_events))])
                
                for i, eid in enumerate(gw_events):
                    with day_tabs[i]:
                        if eid in past_event_ids:
                            st.caption(f"Status: COMPLETED | Date: {event_dates.get(eid, '?')}")
                            if eid in past_day_stats:
                                stats = past_day_stats[eid]
                                st.metric("Daily Score", f"{stats['score']:.1f}")
                                roster_list = []
                                for pick in stats['picks']:
                                    pid = pick['element']
                                    p_row = active_players.loc[active_players['id'] == pid]
                                    name = p_row['web_name'].values[0] if not p_row.empty else "Unknown"
                                    team_short = p_row['team_short'].values[0] if not p_row.empty else "-"
                                    role = "Starter"
                                    if pick['multiplier'] == 0: role = "Bench"
                                    # FIX: Check multiplier > 1 for actual captain usage
                                    if pick['is_captain'] and pick['multiplier'] > 1: role = "CAPTAIN ⭐"
                                    # FIX: Divide points by 10 for table
                                    pts = pick.get('points', 0) / 10.0
                                    roster_list.append({"Name": name, "Team": team_short, "Role": role, "Score": f"{pts:.1f}"})
                                st.dataframe(pd.DataFrame(roster_list), use_container_width=True, hide_index=True)
                            else: st.info("No data.")
                        
                        elif eid in future_event_ids:
                            # FIX: Use consistent variable name
                            d_idx = event_id_to_solver_idx[eid]
                            st.caption(f"Status: UPCOMING | Date: {event_dates.get(eid, '?')}")
                            
                            roster_today = []
                            roster_ids = set()
                            for p in players_data:
                                if roster_vars[(p['id'], d_idx)].varValue > 0.5:
                                    roster_today.append(p)
                                    roster_ids.add(p['id'])
                            
                            trans_in = roster_ids - previous_roster_ids
                            trans_out = previous_roster_ids - roster_ids
                            
                            if trans_in:
                                st.subheader("Transfers")
                                c1, c2 = st.columns(2)
                                with c1:
                                    for pid in trans_out:
                                        p_obj = next((x for x in players_data if x['id'] == pid), None)
                                        name = p_obj['name'] if p_obj else "Unknown"
                                        cost = p_obj['cost']/10 if p_obj else "?"
                                        st.error(f"OUT: {name} (Sell: {cost}m)")
                                with c2:
                                    for pid in trans_in:
                                        p_obj = next(x for x in players_data if x['id'] == pid)
                                        st.success(f"IN: {p_obj['name']} (Buy: {p_obj['cost']/10}m)")
                            
                            previous_roster_ids = roster_ids
                            
                            lineup_data = []
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
                                lineup_data.append({
                                    "Name": p['name'], "Team": p['team_short'],
                                    "Pos": p['pos'], "Value": f"{p['current_val']/10}m",
                                    "Role": status, "Exp Pts": f"{points:.1f}"
                                })
                            
                            df = pd.DataFrame(lineup_data)
                            role_order = {"CAPTAIN ⭐": 0, "Starter": 1, "Bench": 2, "No Game": 3}
                            df['sort_key'] = df['Role'].map(role_order)
                            df = df.sort_values('sort_key').drop('sort_key', axis=1)
                            st.dataframe(df, use_container_width=True, hide_index=True)