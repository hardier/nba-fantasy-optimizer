import streamlit as st
import requests
import pandas as pd
import pulp
import time
from datetime import datetime
import math

# --- CONFIGURATION ---
BASE_URL = "https://nbafantasy.nba.com/api"
DEFAULT_TEAM_ID = 17
DEFAULT_GAMEWEEK = 7

POSITIONS = {"Back Court": 5, "Front Court": 5}
MAX_PLAYERS_PER_TEAM = 2
TRANSFERS_ALLOWED = 2
ROSTER_SIZE = 10

st.set_page_config(page_title="NBA Fantasy Optimizer", layout="wide")

# --- HELPER FUNCTIONS ---

def fetch_json(url):
    """Helper to fetch JSON data."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        return None

@st.cache_data(ttl=3600)
def fetch_bootstrap():
    """Fetches general game data (players, teams, events, phases)."""
    return fetch_json(f"{BASE_URL}/bootstrap-static/")

@st.cache_data(ttl=3600)
def fetch_fixtures():
    """Fetches all fixtures."""
    return fetch_json(f"{BASE_URL}/fixtures/")

def get_gameweek_event_range(bootstrap, gameweek):
    """
    Dynamically finds the start and stop event IDs for a given Gameweek
    using the 'phases' data from bootstrap-static.
    """
    phases = bootstrap.get('phases', [])
    target_phase = None
    gw_name = f"Gameweek {gameweek}"
    
    for phase in phases:
        if phase['name'] == gw_name:
            target_phase = phase
            break
            
    if not target_phase:
        return []

    start = target_phase['start_event']
    stop = target_phase['stop_event']
    return list(range(start, stop + 1))

def fetch_my_team(team_id, event_id):
    """Fetches user's picks and bank."""
    url = f"{BASE_URL}/entry/{team_id}/event/{event_id}/picks/"
    data = fetch_json(url)
    
    # Fallback to previous event if current hasn't started
    if not data:
        url = f"{BASE_URL}/entry/{team_id}/event/{event_id-1}/picks/"
        data = fetch_json(url)
        
    return data

def calculate_selling_price(purchase_price, now_cost):
    """
    Calculates selling price based on 50% sell-on fee logic.
    Fee is 50% of profit, rounded up to nearest 0.1m.
    Prices are in integer units (e.g., 52 = 5.2m).
    """
    if now_cost <= purchase_price:
        return now_cost
    
    profit = now_cost - purchase_price
    # Fee is 50% of profit, rounded up.
    fee = math.ceil(profit / 2)
    return now_cost - fee

@st.cache_data(ttl=86400) # Cache player stats for 24 hours
def get_player_history_avg(player_id):
    """
    Calculates avg points for last 10 active games.
    Returns None if player is considered injured (0 mins in last 2 games).
    """
    url = f"{BASE_URL}/element-summary/{player_id}/"
    data = fetch_json(url)
    if not data: return 0.0
    
    history = data.get('history', [])
    history.sort(key=lambda x: x['kickoff_time'], reverse=True)
    
    # Injury Check: 0 mins in last 2 recorded games
    if len(history) >= 2:
        m1 = int(history[0].get('minutes', 0))
        m2 = int(history[1].get('minutes', 0))
        if m1 == 0 and m2 == 0:
            return None

    # Avg Calculation
    played_games = [g for g in history if int(g.get('minutes', 0)) > 0]
    last_10 = played_games[:10]
    
    if not last_10:
        return 0.0
    
    total_points = sum(g['total_points'] for g in last_10)
    return total_points / len(last_10)

# --- APP UI ---

st.title("🏀 NBA Fantasy Optimizer")
st.markdown("Optimize your lineup and transfer strategy based on **Expected Points** (Last 10 Avg).")

# Sidebar Inputs
with st.sidebar:
    st.header("Settings")
    team_id_input = st.number_input("Team ID (0 for 100m scratch)", value=DEFAULT_TEAM_ID, step=1)
    gameweek_input = st.number_input("Gameweek", value=DEFAULT_GAMEWEEK, step=1)
    
    # Safety margin for budget in 0.1m units (e.g., 1 = 0.1m)
    safety_margin = st.number_input("Budget Safety Margin (0.1m units)", value=1, min_value=0, step=1, help="Reserve this amount to avoid calculation errors.")
    
    run_btn = st.button("Run Optimization", type="primary")

if run_btn:
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    # 1. Fetch Static Data
    status_text.text("Fetching player database...")
    bootstrap = fetch_bootstrap()
    if not bootstrap:
        st.error("Failed to fetch static data.")
        st.stop()
        
    elements = pd.DataFrame(bootstrap['elements'])
    teams = pd.DataFrame(bootstrap['teams'])
    element_types = pd.DataFrame(bootstrap['element_types'])
    
    # Process Maps
    team_map = pd.Series(teams.name.values, index=teams.id).to_dict()
    pos_map = pd.Series(element_types.singular_name.values, index=element_types.id).to_dict()
    elements['team_name'] = elements['team'].map(team_map)
    elements['position_name'] = elements['element_type'].map(pos_map)
    elements['full_name'] = elements['first_name'] + " " + elements['second_name']
    
    active_players = elements[elements['status'] != 'u'].copy()
    
    # 2. Determine Gameweek Range (DYNAMICALLY)
    status_text.text(f"Determining schedule for Gameweek {gameweek_input}...")
    target_event_ids = get_gameweek_event_range(bootstrap, gameweek_input)
    
    if not target_event_ids:
        st.error(f"Could not find schedule for 'Gameweek {gameweek_input}' in API data.")
        st.stop()
        
    roster_source_event_id = min(target_event_ids) - 1
    st.info(f"Optimization for Gameweek {gameweek_input} (Events {min(target_event_ids)}-{max(target_event_ids)})")
    
    # 3. Fetch User Team & Calculate Budget
    my_player_ids = []
    my_selling_prices = {}
    
    if team_id_input == 0:
        st.success("✨ Generating optimal starting lineup from scratch (Budget: 100m)")
        # 100m = 1000 units
        total_budget_safe = 1000
    else:
        status_text.text(f"Fetching Roster for Team {team_id_input}...")
        my_team_data = fetch_my_team(team_id_input, roster_source_event_id)
        
        if not my_team_data:
            st.error(f"Could not fetch data for Team ID {team_id_input}. Check ID or Gameweek.")
            st.stop()
            
        picks = my_team_data['picks']
        my_bank = my_team_data['entry_history']['bank']
        
        current_roster_liquidation_value = 0
        
        # Display Current Roster Details
        with st.expander("Current Roster Valuation", expanded=False):
            roster_data = []
            for p in picks:
                pid = p['element']
                my_player_ids.append(pid)
                p_row = active_players.loc[active_players['id'] == pid]
                
                if p_row.empty: continue
                
                now_cost = p_row['now_cost'].values[0]
                purchase_price = p.get('purchase_price', now_cost)
                sell_price = calculate_selling_price(purchase_price, now_cost)
                
                my_selling_prices[pid] = sell_price
                current_roster_liquidation_value += sell_price
                
                roster_data.append({
                    "Player": p_row['web_name'].values[0],
                    "Current Price": f"{now_cost/10}m"
                })
            st.dataframe(pd.DataFrame(roster_data))

        # Calculate Total Safe Budget
        total_budget_raw = current_roster_liquidation_value + my_bank
        total_budget_safe = total_budget_raw - safety_margin
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Liquidation Value", f"{current_roster_liquidation_value/10}m")
        col2.metric("Bank", f"{my_bank/10}m")
        col3.metric("Optimizer Budget", f"{total_budget_safe/10}m")
    
    # 4. Fetch Fixtures
    status_text.text("Fetching fixtures...")
    fixtures_data = fetch_fixtures()
    fixtures = pd.DataFrame(fixtures_data)
    gw_fixtures = fixtures[fixtures['event'].isin(target_event_ids)].copy()
    gw_fixtures['day_date'] = pd.to_datetime(gw_fixtures['kickoff_time']).dt.date
    game_days = sorted(gw_fixtures['day_date'].unique())
    
    # 5. Calculate Stats
    status_text.text("Calculating player stats (Last 10 Avg)...")
    
    top_candidates = active_players.sort_values('total_points', ascending=False).head(200)
    candidate_ids = set(top_candidates['id'].tolist())
    for pid in my_player_ids:
        candidate_ids.add(pid)
        
    players_to_fetch = active_players[active_players['id'].isin(candidate_ids)]
    player_eps = {}
    
    total_players = len(players_to_fetch)
    
    for i, (index, player) in enumerate(players_to_fetch.iterrows()):
        if i % 10 == 0:
            progress_bar.progress(int((i / total_players) * 90))
            
        pid = player['id']
        avg = get_player_history_avg(pid)
        
        # Keep owned players even if injured (0 pts) to prevent crash
        if avg is None:
            if pid in my_player_ids:
                avg = 0.0
            else:
                continue
                
        player_eps[pid] = avg

    progress_bar.progress(95)
    status_text.text("Optimizing lineup...")

    # 6. Optimization Model
    prob = pulp.LpProblem("NBA_Fantasy", pulp.LpMaximize)
    
    players_data = []
    for pid, ep in player_eps.items():
        p_row = elements[elements['id'] == pid].iloc[0]
        pos = p_row['position_name']
        simple_pos = "Back Court" if ("Guard" in pos or "Back" in pos) else "Front Court"
        
        # Cost Logic: Owned = Selling Price, New = Market Price
        if pid in my_selling_prices:
            effective_cost = my_selling_prices[pid]
        else:
            effective_cost = p_row['now_cost']
        
        players_data.append({
            'id': pid,
            'name': p_row['web_name'],
            'full_name': p_row['full_name'],
            'cost': effective_cost,
            'pos': simple_pos,
            'team': p_row['team'],
            'ep': ep
        })
        
    # Variables
    roster_vars = {} 
    trans_in_vars = {}
    starter_vars = {} 
    captain_vars = {}
    
    player_schedule = {p['id']: set() for p in players_data}
    for _, f in gw_fixtures.iterrows():
        for p in players_data:
            if p['team'] == f['team_h'] or p['team'] == f['team_a']:
                player_schedule[p['id']].add(f['day_date'])

    for d_idx, day in enumerate(game_days):
        for p in players_data:
            pid = p['id']
            roster_vars[(pid, d_idx)] = pulp.LpVariable(f"Roster_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
            if d_idx > 0:
                trans_in_vars[(pid, d_idx)] = pulp.LpVariable(f"In_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
            
            if day in player_schedule[pid]:
                starter_vars[(pid, d_idx)] = pulp.LpVariable(f"Start_{pid}_{d_idx}", 0, 1, pulp.LpBinary)
                captain_vars[(pid, d_idx)] = pulp.LpVariable(f"Capt_{pid}_{d_idx}", 0, 1, pulp.LpBinary)

    # Constraints
    # Day 1 Roster Enforcement (Only if not scratch start)
    if team_id_input != 0:
        for p in players_data:
            if p['id'] in my_player_ids:
                prob += roster_vars[(p['id'], 0)] == 1
            else:
                if len([x for x in my_player_ids if x in player_eps]) == 10:
                    prob += roster_vars[(p['id'], 0)] == 0

    prob += pulp.lpSum(trans_in_vars.values()) <= TRANSFERS_ALLOWED
    prob += pulp.lpSum(captain_vars.values()) == 1
    
    teams_list = elements['team'].unique()
    bc_players = [p for p in players_data if p['pos'] == "Back Court"]
    fc_players = [p for p in players_data if p['pos'] == "Front Court"]
    
    total_obj = 0
    
    for d_idx, day in enumerate(game_days):
        current_roster = [roster_vars[(p['id'], d_idx)] for p in players_data]
        
        prob += pulp.lpSum(current_roster) == ROSTER_SIZE
        prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] * p['cost'] for p in players_data]) <= total_budget_safe
        prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] for p in bc_players]) == 5
        prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] for p in fc_players]) == 5
        
        for t in teams_list:
            t_players = [p for p in players_data if p['team'] == t]
            prob += pulp.lpSum([roster_vars[(p['id'], d_idx)] for p in t_players]) <= MAX_PLAYERS_PER_TEAM

        if d_idx > 0:
            for p in players_data:
                pid = p['id']
                prob += trans_in_vars[(pid, d_idx)] >= roster_vars[(pid, d_idx)] - roster_vars[(pid, d_idx-1)]

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

    prob += total_obj
    prob.solve(pulp.PULP_CBC_CMD(msg=0))
    
    progress_bar.progress(100)
    status_text.text("Done!")
    
    # 7. Results
    if pulp.LpStatus[prob.status] != 'Optimal':
        st.error("Optimization failed. Your current roster might violate strict budget or team limits.")
    else:
        proj_score = pulp.value(prob.objective) / 10
        st.success(f"Optimization Successful! Projected Score: {proj_score:.1f}")
        
        # Use Day 1, Day 2... for tabs instead of exact dates
        tabs = st.tabs([f"Day {i+1}" for i in range(len(game_days))])
        previous_roster = set()
        
        for d_idx, day in enumerate(game_days):
            with tabs[d_idx]:
                st.caption(f"Date: {day}") # Show actual date inside tab
                
                roster_today = []
                roster_ids = set()
                for p in players_data:
                    if roster_vars[(p['id'], d_idx)].varValue > 0.5:
                        roster_today.append(p)
                        roster_ids.add(p['id'])
                
                if d_idx > 0:
                    trans_in = roster_ids - previous_roster
                    trans_out = previous_roster - roster_ids
                    if trans_in:
                        st.subheader("Transfers")
                        c1, c2 = st.columns(2)
                        with c1:
                            for pid in trans_out:
                                p_obj = next(x for x in players_data if x['id'] == pid)
                                # Show actual sell value (which frees up budget)
                                st.error(f"OUT: {p_obj['name']} (Sell: {p_obj['cost']/10}m)")
                        with c2:
                            for pid in trans_in:
                                p_obj = next(x for x in players_data if x['id'] == pid)
                                st.success(f"IN: {p_obj['name']} (Buy: {p_obj['cost']/10}m)")
                
                previous_roster = roster_ids
                
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
                    elif day not in player_schedule[pid]:
                        status = "No Game"
                    
                    lineup_data.append({
                        "Name": p['name'],
                        "Pos": p['pos'],
                        "Role": status,
                        "Exp Pts": f"{points:.1f}"
                    })
                
                df = pd.DataFrame(lineup_data)
                role_order = {"CAPTAIN ⭐": 0, "Starter": 1, "Bench": 2, "No Game": 3}
                df['sort_key'] = df['Role'].map(role_order)
                df = df.sort_values('sort_key').drop('sort_key', axis=1)
                
                st.dataframe(df, use_container_width=True, hide_index=True)