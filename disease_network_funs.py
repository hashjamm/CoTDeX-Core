import numpy as np
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import logging
import pickle
import multiprocessing as mp
from multiprocessing import Pool
import pyreadstat
import re
import networkx as nx
from infomap import Infomap
from collections import defaultdict
from joblib import Parallel, delayed
import pickle
from collections import Counter
import scipy
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle
import math
from matplotlib.patches import FancyArrowPatch
import seaborn as sns

sas_path = "/home/hashjamm/project_data/disease_network/sas_files/"
edge_pids_path = "/home/hashjamm/results/disease_network/edge_pids/"
matched_path = "/home/hashjamm/project_data/disease_network/sas_files/matched/"
ctable_path = "/home/hashjamm/results/disease_network/ctables/"
pids_info_path = "/home/hashjamm/results/disease_network/"
final_results_path = "/home/hashjamm/results/disease_network/final_results/"
network_path = "/home/hashjamm/results/disease_network/default_network_properties/"

# def process_disease_pair(cause_abb, diseases_list, all_outcome_np):
    
#     local_cause_abb_list = []
#     local_outcome_abb_list = []
#     local_ct00_list = []
#     local_ct01_list = []
#     local_ct10_list = []
#     local_ct11_list = []
    
#     # cause 데이터 불러오기 및 NumPy 변환
#     cause_df = pyreadstat.read_sas7bdat(f'{matched_path}matched_{str(cause_abb).lower()}.sas7bdat')[0]
    
#     if 'case' in cause_df.columns:
#         cause_df = cause_df[['PERSON_ID', 'case']].rename(columns={'case': 'cause'})
    
#     cause_np = cause_df[['PERSON_ID', 'cause']].values
    
#     for outcome_abb in diseases_list:
        
#         if cause_abb == outcome_abb: 
#             continue
        
#         # outcome 데이터 필터링
#         outcome_np = all_outcome_np[all_outcome_np[:, 1] == outcome_abb][:, [0]]
#         outcome_np = np.hstack([outcome_np, np.ones((outcome_np.shape[0], 1), dtype=int)])

#         # PERSON_ID 기준으로 병합 (NumPy 방식)
#         one_final_np = np.zeros((cause_np.shape[0], 3), dtype=int)
#         one_final_np[:, 0:2] = cause_np  # cause (PERSON_ID, cause)

#         person_id_cause = set(cause_np[:, 0])
#         person_id_outcome = set(outcome_np[:, 0])
        
#         match_ids = person_id_cause.intersection(person_id_outcome)
        
#         if not match_ids:
#             continue
        
#         match_outcome_indices = np.isin(cause_np[:, 0], list(match_ids))
#         one_final_np[match_outcome_indices, 2] = 1  # outcome status

#         # 교차 테이블 (crosstab 대체)
#         cause_outcome_matrix = np.zeros((2, 2), dtype=int)
#         for row in one_final_np:
#             cause_outcome_matrix[row[1], row[2]] += 1
        
#         ct00, ct01, ct10, ct11 = cause_outcome_matrix.flatten()
        
#         # 조건 확인 및 결과 저장
#         if min(ct00, ct01, ct10, ct11) < 5:
#             continue
        
#         local_cause_abb_list.append(cause_abb)
#         local_outcome_abb_list.append(outcome_abb)
#         local_ct00_list.append(ct00)
#         local_ct01_list.append(ct01)
#         local_ct10_list.append(ct10)
#         local_ct11_list.append(ct11)
        
#     return local_cause_abb_list, local_outcome_abb_list, local_ct00_list, local_ct01_list, local_ct10_list, local_ct11_list
    
# def process_disease_pair_unfiltered(cause_abb, diseases_list, all_outcome_np):
    
#     local_cause_abb_list = []
#     local_outcome_abb_list = []
#     local_ct00_list = []
#     local_ct01_list = []
#     local_ct10_list = []
#     local_ct11_list = []

#     # cause 데이터 불러오기 및 NumPy 변환
#     cause_df = pyreadstat.read_sas7bdat(f'{matched_path}matched_{str(cause_abb).lower()}.sas7bdat')[0]
    
#     if 'case' in cause_df.columns:
#         cause_df = cause_df[['PERSON_ID', 'case']].rename(columns={'case': 'cause'})
    
#     cause_np = cause_df[['PERSON_ID', 'cause']].values
    
#     case_number = np.sum(cause_np[:, 1] == 1)
#     control_number = np.sum(cause_np[:, 1] == 0)
    
#     for outcome_abb in diseases_list:
        
#         if cause_abb == outcome_abb: 
#             continue
        
#         # outcome 데이터 필터링
#         outcome_np = all_outcome_np[all_outcome_np[:, 1] == outcome_abb][:, [0]]
        
#         if outcome_np.size == 0:
            
#             local_cause_abb_list.append(cause_abb)
#             local_outcome_abb_list.append(outcome_abb)
#             local_ct00_list.append(control_number)
#             local_ct01_list.append(0)
#             local_ct10_list.append(case_number)
#             local_ct11_list.append(0)
        
#         else:
        
#             outcome_np = np.hstack([outcome_np, np.ones((outcome_np.shape[0], 1), dtype=int)])

#             person_id_cause = set(cause_np[:, 0])
#             person_id_outcome = set(outcome_np[:, 0])

#             match_ids = person_id_cause.intersection(person_id_outcome)

#             if not match_ids:
                
#                 local_cause_abb_list.append(cause_abb)
#                 local_outcome_abb_list.append(outcome_abb)
#                 local_ct00_list.append(control_number)
#                 local_ct01_list.append(0)
#                 local_ct10_list.append(case_number)
#                 local_ct11_list.append(0)
                
#             else:

#                 # PERSON_ID 기준으로 병합 (NumPy 방식)
#                 one_final_np = np.zeros((cause_np.shape[0], 3), dtype=int)
#                 one_final_np[:, 0:2] = cause_np  # cause (PERSON_ID, cause)

#                 match_outcome_indices = np.isin(cause_np[:, 0], list(match_ids))
#                 one_final_np[match_outcome_indices, 2] = 1  # outcome status
                
#                 # 교차 테이블 (crosstab 대체)
#                 cause_outcome_matrix = np.zeros((2, 2), dtype=int)
#                 for row in one_final_np:
#                     cause_outcome_matrix[row[1], row[2]] += 1

#                 ct00, ct01, ct10, ct11 = cause_outcome_matrix.flatten()

#                 local_cause_abb_list.append(cause_abb)
#                 local_outcome_abb_list.append(outcome_abb)
#                 local_ct00_list.append(ct00)
#                 local_ct01_list.append(ct01)
#                 local_ct10_list.append(ct10)
#                 local_ct11_list.append(ct11)
        
#     return local_cause_abb_list, local_outcome_abb_list, local_ct00_list, local_ct01_list, local_ct10_list, local_ct11_list

def process_disease_pair_unfiltered(cause_abb, diseases_list, all_outcome_np):
    
    local_cause_abb_list = []
    local_outcome_abb_list = []
    local_ct00_list = []
    local_ct01_list = []
    local_ct10_list = []
    local_ct11_list = []
    local_edge_pids_dict = {}

    # cause 데이터 불러오기 및 NumPy 변환
    cause_df = pyreadstat.read_sas7bdat(f'{matched_path}matched_{str(cause_abb).lower()}.sas7bdat')[0]
    
    if 'case' in cause_df.columns:
        cause_df = cause_df[['PERSON_ID', 'case']].rename(columns={'case': 'cause'})
    
    cause_np = cause_df[['PERSON_ID', 'cause']].values
    
    case_number = np.sum(cause_np[:, 1] == 1)
    control_number = np.sum(cause_np[:, 1] == 0)
    
    for outcome_abb in diseases_list:
        
        if cause_abb == outcome_abb: 
            continue
        
        # outcome 데이터 필터링
        outcome_np = all_outcome_np[all_outcome_np[:, 1] == outcome_abb][:, [0]]
        
        if outcome_np.size == 0:
            
            local_cause_abb_list.append(cause_abb)
            local_outcome_abb_list.append(outcome_abb)
            local_ct00_list.append(control_number)
            local_ct01_list.append(0)
            local_ct10_list.append(case_number)
            local_ct11_list.append(0)
        
        else:
        
            outcome_np = np.hstack([outcome_np, np.ones((outcome_np.shape[0], 1), dtype=int)])

            person_id_cause = set(cause_np[:, 0])
            person_id_outcome = set(outcome_np[:, 0])

            match_ids = person_id_cause.intersection(person_id_outcome)

            if not match_ids:
                
                local_cause_abb_list.append(cause_abb)
                local_outcome_abb_list.append(outcome_abb)
                local_ct00_list.append(control_number)
                local_ct01_list.append(0)
                local_ct10_list.append(case_number)
                local_ct11_list.append(0)
                
            else:

                # PERSON_ID 기준으로 병합 (NumPy 방식)
                one_final_np = np.zeros((cause_np.shape[0], 3), dtype=int)
                one_final_np[:, 0:2] = cause_np  # cause (PERSON_ID, cause)

                match_outcome_indices = np.isin(cause_np[:, 0], list(match_ids))
                one_final_np[match_outcome_indices, 2] = 1  # outcome status
                
                edge_pids = one_final_np[(one_final_np[:, 1] == 1) & (one_final_np[:, 2] == 1)][:, 0]
                if len(edge_pids) > 0:
                    update_edge_pids_dict = {(cause_abb, outcome_abb): edge_pids}
                    local_edge_pids_dict.update(update_edge_pids_dict)
                
                # 교차 테이블 (crosstab 대체)
                cause_outcome_matrix = np.zeros((2, 2), dtype=int)
                for row in one_final_np:
                    cause_outcome_matrix[row[1], row[2]] += 1

                ct00, ct01, ct10, ct11 = cause_outcome_matrix.flatten()

                local_cause_abb_list.append(cause_abb)
                local_outcome_abb_list.append(outcome_abb)
                local_ct00_list.append(ct00)
                local_ct01_list.append(ct01)
                local_ct10_list.append(ct10)
                local_ct11_list.append(ct11)
        
    return local_cause_abb_list, local_outcome_abb_list, local_ct00_list, local_ct01_list, local_ct10_list, local_ct11_list, local_edge_pids_dict

def updating_disease_pair(previous_ctable_unfiltered_np, post_distinct_ctable_unfilterd_np):
    
    # B의 4열의 값을 A의 3열에서 빼고, A의 4열에서 더하기
    previous_ctable_unfiltered_np[:, 2] -= post_distinct_ctable_unfilterd_np[:, 3]  # A의 3열에 B의 4열을 뺌
    previous_ctable_unfiltered_np[:, 3] += post_distinct_ctable_unfilterd_np[:, 3]  # A의 4열에 B의 4열을 더함

    # B의 6열의 값을 A의 5열에서 빼고, A의 6열에서 더하기
    previous_ctable_unfiltered_np[:, 4] -= post_distinct_ctable_unfilterd_np[:, 5]  # A의 5열에 B의 6열을 뺌
    previous_ctable_unfiltered_np[:, 5] += post_distinct_ctable_unfilterd_np[:, 5]  # A의 6열에 B의 6열을 더함
    
    updated_df = pd.DataFrame(previous_ctable_unfiltered_np, columns=['cause_abb', 'outcome_abb', 'ct00', 'ct01', 'ct10', 'ct11'])
        
    return updated_df

def filtered_pair_cont_part_extract(updated_need_cause):

    local_cause_abb_list = []
    local_outcome_abb_list = []
    local_ct00_list = []
    local_ct01_list = []
    local_ct10_list = []
    local_ct11_list = []

    # cause 데이터 불러오기 및 NumPy 변환
    cause_df = pyreadstat.read_sas7bdat(f'{matched_path}matched_{str(updated_need_cause).lower()}.sas7bdat')[0]

    if 'case' in cause_df.columns:
        cause_df = cause_df[['PERSON_ID', 'case']].rename(columns={'case': 'cause'})

    cause_np = cause_df[['PERSON_ID', 'cause']].values

    case_number = np.sum(cause_np[:, 1] == 1)
    control_number = np.sum(cause_np[:, 1] == 0)

    local_cause_abb_list.append(updated_need_cause)
    local_ct00_list.append(control_number)
    local_ct01_list.append(0)
    local_ct10_list.append(case_number)
    local_ct11_list.append(0)
    
    return local_cause_abb_list, local_ct00_list, local_ct01_list, local_ct10_list, local_ct11_list

def full_to_final(full_ctable, return_option=True, save_option=False, save_path=None):
    final_ctable = full_ctable.loc[
        (full_ctable['ct00'] >= 5) &
        (full_ctable['ct01'] >= 5) &
        (full_ctable['ct10'] >= 5) &
        (full_ctable['ct11'] >= 5)
    ]
    
    final_ctable = final_ctable.reset_index(drop=True)
    
    if save_option and save_path is not None:
        final_ctable.to_csv(save_path, index=False)
    
    if return_option:
        return final_ctable

def make_counts_dict(df: pd.DataFrame, collist: list, prefix_list: list):
    col_num = len(collist)
    
    if col_num != len(prefix_list):
        raise ValueError("Error: 'collist'와 'prefix_list'의 길이가 일치하지 않습니다.")
    
    if col_num == 0:
        raise ValueError("Error: 'collist' 길이가 0입니다.")
    
    if not all(col in df.columns for col in collist):
        raise ValueError("Error: 'collist'에 포함된 일부 컬럼이 데이터프레임에 없습니다.")

    if col_num == 1:
        counts_dict = df[collist[0]].value_counts().to_dict()
        try:
            counts_dict = dict(sorted(counts_dict.items(), key=lambda item: int(item[0])))
        except ValueError:
            print("정수로 변환할 수 없는 key가 존재합니다.")
            raise

        counts_dict = {
            f"{prefix_list[0]}_{k}_counts": v for k, v in counts_dict.items()
        }
        return counts_dict

    else:
        counts_dict = df[collist].value_counts().to_dict()
        try:
            counts_dict = dict(
                sorted(counts_dict.items(), key=lambda item: tuple(map(int, item[0])))
            )
        except ValueError:
            print("정수로 변환할 수 없는 key가 존재합니다.")
            raise

        # 동적 key 이름 구성
        renamed_dict = {}
        for k, v in counts_dict.items():
            key_parts = [
                f"{prefix_list[i]}_{k[i]}" for i in range(col_num)
            ]
            final_key = "_".join(key_parts) + "_counts"
            renamed_dict[final_key] = v

        return renamed_dict
    
def merge_dicts(*dicts):
    result = {}
    for d in dicts:
        result.update(d)
    return result
    
def node_pids_info_extractor(cause_abb):
    
    matched_df = pyreadstat.read_sas7bdat(f'{matched_path}matched_{str(cause_abb).lower()}.sas7bdat')[0]
    matched_df[['PERSON_ID', 'case']] = matched_df[['PERSON_ID', 'case']].astype(int)
    
    target_df = matched_df[matched_df['case'] == 1].copy()
    target_df[['PERSON_ID', 'SEX', 'AGE_GROUP', 'SGG', 'CTRB_PT_TYPE_CD', 'case']] =\
    target_df[['PERSON_ID', 'SEX', 'AGE_GROUP', 'SGG', 'CTRB_PT_TYPE_CD', 'case']].astype('category')

    target_df['SIDO'] = target_df['SGG'].str[:2]

    # 단일 컬럼
    sex_counts_dict = make_counts_dict(target_df, ['SEX'], ['sex'])
    age_counts_dict = make_counts_dict(target_df, ['AGE_GROUP'], ['age'])
    sido_counts_dict = make_counts_dict(target_df, ['SIDO'], ['sido'])
    # sgg_counts_dict = make_counts_dict(target_df, ['SGG'], ['sgg'])
    ctrb_counts_dict = make_counts_dict(target_df, ['CTRB_PT_TYPE_CD'], ['ctrb'])

    # 다중 컬럼
    sex_age_counts_dict = make_counts_dict(target_df, ['SEX', 'AGE_GROUP'], ['sex', 'age'])
    sex_sido_counts_dict = make_counts_dict(target_df, ['SEX', 'SIDO'], ['sex', 'sido'])
    # sex_sgg_counts_dict = make_counts_dict(target_df, ['SEX', 'SGG'], ['sex', 'sgg'])
    sex_ctrb_counts_dict = make_counts_dict(target_df, ['SEX', 'CTRB_PT_TYPE_CD'], ['sex', 'ctrb'])

    combined_dict = merge_dicts({'node_code': f'{cause_abb}'},\
                                sex_counts_dict,\
                                age_counts_dict,\
                                sido_counts_dict,\
                                # sgg_counts_dict,\
                                ctrb_counts_dict,\
                                sex_age_counts_dict,\
                                sex_sido_counts_dict,\
                                # sex_sgg_counts_dict,\
                                sex_ctrb_counts_dict)
    
    return combined_dict

def move_column(df: pd.DataFrame, column_name, new_index):
    # 컬럼 제거 후 삽입
    cols = list(df.columns)
    cols.insert(new_index, cols.pop(cols.index(column_name)))
    return df[cols]

def node_shape_col_maker(node_pids_info: pd.DataFrame):
    
    node_pids_info['width'] = node_pids_info['sex_2_counts'].rank(method='min') / len(node_pids_info)
    node_pids_info['height'] = node_pids_info['sex_1_counts'].rank(method='min') / len(node_pids_info)
    
    node_pids_info = move_column(node_pids_info, 'width', 1)
    node_pids_info = move_column(node_pids_info, 'height', 2)
    
    return node_pids_info
    
all_std_info = pyreadstat.read_sas7bdat(f'{sas_path}std_pop4.sas7bdat')[0]
all_std_info['PERSON_ID'] = all_std_info['PERSON_ID'].astype(int)
all_std_info = all_std_info.drop(columns = ['case'])

def edge_pids_info_extractor(one_edge_pids_item: tuple):
    
    if len(one_edge_pids_item) != 2:
        raise ValueError("Error: one_edge_pids_item - tuple 길이가 2가 아닙니다.")
    
    edge_pids_key = one_edge_pids_item[0]
    edge_pids_value = one_edge_pids_item[1]
    
    if len(edge_pids_key) != 2:
        raise ValueError("Error: edge_pids_key - tuple 길이가 2가 아닙니다.")
        
    if any(not re.fullmatch(r"[A-Z][0-9]{2}", item) for item in edge_pids_key):
        raise ValueError("Error: edge_pids_key - tuple 내부에 형식에 맞지 않는 값이 존재합니다.")
        
    cause_abb = edge_pids_key[0]
    outcome_abb = edge_pids_key[1]
        
    target_df = all_std_info.loc[all_std_info['PERSON_ID'].isin(edge_pids_value)].reset_index(drop=True)
    target_df['SIDO'] = target_df['SGG'].str[:2]
    
    # 단일 컬럼
    sex_counts_dict = make_counts_dict(target_df, ['SEX'], ['sex'])
    age_counts_dict = make_counts_dict(target_df, ['AGE_GROUP'], ['age'])
    sido_counts_dict = make_counts_dict(target_df, ['SIDO'], ['sido'])
    # sgg_counts_dict = make_counts_dict(target_df, ['SGG'], ['sgg'])
    ctrb_counts_dict = make_counts_dict(target_df, ['CTRB_PT_TYPE_CD'], ['ctrb'])

    # 다중 컬럼
    sex_age_counts_dict = make_counts_dict(target_df, ['SEX', 'AGE_GROUP'], ['sex', 'age'])
    sex_sido_counts_dict = make_counts_dict(target_df, ['SEX', 'SIDO'], ['sex', 'sido'])
    # sex_sgg_counts_dict = make_counts_dict(target_df, ['SEX', 'SGG'], ['sex', 'sgg'])
    sex_ctrb_counts_dict = make_counts_dict(target_df, ['SEX', 'CTRB_PT_TYPE_CD'], ['sex', 'ctrb'])

    combined_dict = merge_dicts({'cause_abb': f'{cause_abb}', 'outcome_abb': f'{outcome_abb}'},\
                                sex_counts_dict,\
                                age_counts_dict,\
                                sido_counts_dict,\
                                # sgg_counts_dict,\
                                ctrb_counts_dict,\
                                sex_age_counts_dict,\
                                sex_sido_counts_dict,\
                                # sex_sgg_counts_dict,\
                                sex_ctrb_counts_dict)
    
    return combined_dict

def colname_type_setting(df: pd.DataFrame, set_start_col_idx: int, prefix: str):
    
    if set_start_col_idx < 0:
        raise ValueError("Error: set_start_col_idx 는 0 이상이여야 합니다.")
        
    df_copy = df.copy(deep=True)
    collist = list(df_copy.columns)
    
    for idx, colname in enumerate(collist):
        if idx < set_start_col_idx:
            continue
        else:
            df_copy[colname] = df_copy[colname].astype('Int64')
            collist[idx] = f'{prefix}_{colname}'
    
    df_copy.columns = collist
    
    return df_copy

def dict_sorting(target_dict: dict):
    result = dict(sorted(target_dict.items(), key=lambda x: x[1], reverse=True))
    return result

def reorder_cluster_ids(cluster_assignments: dict) -> dict:
    cluster_to_nodes = defaultdict(list)
    for node, cluster_id in cluster_assignments.items():
        cluster_to_nodes[cluster_id].append(node)
    sorted_clusters = sorted(cluster_to_nodes.items(), key=lambda x: len(x[1]), reverse=True)
    old_to_new = {old: new for new, (old, _) in enumerate(sorted_clusters)}
    return {node: old_to_new[cluster] for node, cluster in cluster_assignments.items()}
    
def compute_from_file(
    file_path,
    auto_log_transform=True, 
    use_largest_scc_for_clustering=True,
    node_widths: dict = None,
    node_heights: dict = None
):
    df = pd.read_csv(file_path)
    return compute_network_features(
        df,
        auto_log_transform,
        use_largest_scc_for_clustering,
        node_widths,
        node_heights
    )

def build_directed_graph_from_df(
    df: pd.DataFrame,
    weight_col: str = 'log_rr_values'
) -> nx.DiGraph:
    G = nx.DiGraph()
    for _, row in df.iterrows():
        G.add_edge(row['cause_abb'], row['outcome_abb'], weight=row[weight_col])
    return G

def compute_network_features(
    filtered_df: pd.DataFrame,
    auto_log_transform=True,
    use_largest_scc_for_clustering=True,
    node_widths: dict = None,
    node_heights: dict = None
) -> dict:
    
    # 엣지 추가
    weight_col = 'log_rr_values' if auto_log_transform else 'rr_values'
    G = build_directed_graph_from_df(filtered_df, weight_col=weight_col)

    # 노드 크기 속성 추가 (선택적)
    if node_widths:
        nx.set_node_attributes(G, node_widths, name='width')
    if node_heights:
        nx.set_node_attributes(G, node_heights, name='height')
    
    num_nodes = G.number_of_nodes()
    num_edges = G.number_of_edges()
    density = nx.density(G)
    is_strongly_connected = nx.is_strongly_connected(G)
    scc_list = list(nx.strongly_connected_components(G))
    num_scc = len(scc_list)
    largest_scc_nodes = max(scc_list, key=len)
    largest_scc_size = len(largest_scc_nodes)
    G_scc = G.subgraph(largest_scc_nodes).copy()

    # 중심성 지표 (전체 그래프 기준)
    degree_centrality = dict_sorting(nx.degree_centrality(G))
    in_degree = dict_sorting(dict(G.in_degree()))
    in_degree_centrality = dict_sorting(nx.in_degree_centrality(G))
    out_degree = dict_sorting(dict(G.out_degree()))
    out_degree_centrality = dict_sorting(nx.out_degree_centrality(G))
    in_strength = dict_sorting(dict(G.in_degree(weight='weight')))
    out_strength = dict_sorting(dict(G.out_degree(weight='weight')))
    pagerank = dict_sorting(nx.pagerank(G, weight='weight'))
    betweenness = dict_sorting(nx.betweenness_centrality(G, weight='weight'))

    # 거리 지표 (largest SCC 기준)
    try:
        avg_shortest_path = nx.average_shortest_path_length(G_scc)
        diameter = nx.diameter(G_scc.to_undirected())
    except:
        avg_shortest_path = None
        diameter = None

    # 클러스터링 대상 설정
    if use_largest_scc_for_clustering:
        clustering_graph = G_scc
        scc_nodes = list(G_scc.nodes())
    else:
        clustering_graph = G
        scc_nodes = list(G.nodes())  # 전체 노드 반환

    # Infomap 클러스터링
    im = Infomap()
    node_to_id = {node: idx for idx, node in enumerate(clustering_graph.nodes())}
    id_to_node = {idx: node for node, idx in node_to_id.items()}

    for u, v, data in clustering_graph.edges(data=True):
        weight = float(data.get('weight', 1.0))
        im.add_link(node_to_id[u], node_to_id[v], weight)

    im.run()

    cluster_assignments_raw = {
        id_to_node[node.node_id]: node.module_id
        for node in im.nodes
    }
    cluster_assignments_sub = reorder_cluster_ids(cluster_assignments_raw)

    # 전체 노드에 대해 클러스터 결과를 매핑
    if use_largest_scc_for_clustering:
        cluster_assignments = {
            node: cluster_assignments_sub.get(node, None) for node in G.nodes()
        }
    else:
        cluster_assignments = cluster_assignments_sub

    # 보조 정보
    num_clustered_nodes = len([v for v in cluster_assignments.values() if v is not None])
    non_clustered_nodes = [node for node, cluster in cluster_assignments.items() if cluster is None]

    return {
        'num_nodes': num_nodes,
        'num_edges': num_edges,
        'density': density,
        'is_strongly_connected': is_strongly_connected,
        'num_strongly_connected_components': num_scc,
        'largest_scc_size': largest_scc_size,
        'avg_shortest_path_length_largest_scc': avg_shortest_path,
        'diameter_largest_scc': diameter,
        'degree_centrality': degree_centrality,
        'in_degree': in_degree,
        'in_degree_centrality': in_degree_centrality,
        'out_degree': out_degree,
        'out_degree_centrality': out_degree_centrality,
        'in_strength': in_strength,
        'out_strength': out_strength,
        'pagerank': pagerank,
        'betweenness': betweenness,
        'cluster_assignments': cluster_assignments,
        'scc_nodes': scc_nodes,
        'non_clustered_nodes': non_clustered_nodes,
        'num_clustered_nodes': num_clustered_nodes,
        'graph': G
    }

def get_top_percent(df: pd.DataFrame, col: str, percent: float) -> pd.DataFrame:
    """
    df: 대상 데이터프레임
    col: 기준이 되는 컬럼명 (str)
    percent: 상위 몇 %인지 (예: 10% → 10.0)

    return: 상위 percent%에 해당하는 행들로 구성된 DataFrame
    """
    if percent <= 0 or percent > 100:
        raise ValueError("percent 값은 0보다 크고 100 이하여야 합니다.")

    threshold = df[col].quantile(1 - percent / 100)
    return df[df[col] >= threshold]

# def visualize_clusters_from_result(
#     result: dict,
#     layout: str = "spring",
#     show_labels: bool = False,
#     show_legend: bool = True,
#     figsize=(12, 10)
# ):

#     G = result['graph']  # ✅ 여기서 직접 가져옴
#     cluster_assignments = result['cluster_assignments']

#     all_clusters = set(v for v in cluster_assignments.values() if v is not None)
#     color_map = {cid: plt.cm.tab20(i % 20) for i, cid in enumerate(sorted(all_clusters))}

#     if layout == 'kamada':
#         pos = nx.kamada_kawai_layout(G)
#     elif layout == 'circular':
#         pos = nx.circular_layout(G)
#     else:
#         pos = nx.spring_layout(G, seed=42)

#     fig, ax = plt.subplots(figsize=figsize)

#    # 클러스터 노드 출력 (직사각형 패치)
#     for cid in sorted(all_clusters):
#         nodes_in_cluster = [node for node, c in cluster_assignments.items() if c == cid]
#         for node in nodes_in_cluster:
#             x, y = pos[node]
#             width = G.nodes[node].get('width', 0.1)
#             height = G.nodes[node].get('height', 0.1)
#             color = color_map[cid]

#             rect = Rectangle(
#                 (x - width / 2, y - height / 2),
#                 width, height,
#                 facecolor=color,
#                 edgecolor='black',
#                 linewidth=0.5,
#                 alpha=0.8
#             )
#             ax.add_patch(rect)

#             if show_labels:
#                 ax.text(x, y, node, ha='center', va='center', fontsize=6)

#     # 비클러스터 노드
#     none_nodes = [node for node, c in cluster_assignments.items() if c is None]
#     for node in none_nodes:
#         x, y = pos[node]
#         width = G.nodes[node].get('width', 0.1)
#         height = G.nodes[node].get('height', 0.1)

#         rect = Rectangle(
#             (x - width / 2, y - height / 2),
#             width, height,
#             facecolor='lightgray',
#             edgecolor='black',
#             linewidth=0.5,
#             alpha=0.5
#         )
#         ax.add_patch(rect)

#         if show_labels:
#             ax.text(x, y, node, ha='center', va='center', fontsize=6)

#     edges = G.edges(data=True)
#     edge_weights = [data.get('weight', 1.0) for _, _, data in edges]
#     edge_widths = [max(0.5, min(4.0, w * 1.5)) for w in edge_weights]
#     nx.draw_networkx_edges(G, pos, width=edge_widths, alpha=0.1)

#     if show_labels:
#         nx.draw_networkx_labels(G, pos, font_size=7)

#     if show_legend:
#         handles = [
#             mpatches.Patch(color=color_map[cid],
#                            label=f"Cluster {cid} (n={len([n for n in cluster_assignments if cluster_assignments[n] == cid])})")
#             for cid in sorted(all_clusters)
#         ]
#         if none_nodes:
#             handles.append(mpatches.Patch(color='lightgray', label=f'Unclustered (n={len(none_nodes)})'))
#         plt.legend(handles=handles, loc='best')

#     plt.title("Network Cluster Visualization")
#     plt.axis('off')
#     plt.tight_layout()
#     plt.show()

# def visualize_clusters_from_result(
#     result: dict,
#     layout: str = "spring",
#     scc: True,
#     show_labels: bool = False,
#     show_legend: bool = True,
#     figsize=(12, 10)
# ):
#     G = result['graph']
#     cluster_assignments = result['cluster_assignments']

#     # 레이아웃 좌표 생성
#     if layout == 'kamada':
#         pos = nx.kamada_kawai_layout(G)
#     elif layout == 'circular':
#         pos = nx.circular_layout(G)
#     else:
#         pos = nx.spring_layout(G, seed=42)

#     # 1. 좌표 정규화
#     x_vals = [p[0] for p in pos.values()]
#     y_vals = [p[1] for p in pos.values()]
#     x_min, x_max = min(x_vals), max(x_vals)
#     y_min, y_max = min(y_vals), max(y_vals)
#     x_range = x_max - x_min
#     y_range = y_max - y_min

#     def normalize_position(x, y):
#         nx = (x - x_min) / x_range if x_range > 0 else 0.5
#         ny = (y - y_min) / y_range if y_range > 0 else 0.5
#         return nx, ny

#     # 클러스터 색상 정의
#     all_clusters = set(v for v in cluster_assignments.values() if v is not None)
#     color_map = {cid: plt.cm.tab20(i % 20) for i, cid in enumerate(sorted(all_clusters))}

#     fig, ax = plt.subplots(figsize=figsize)

#     # 2. 클러스터 노드 시각화 (직사각형)
#     for cid in sorted(all_clusters):
#         nodes_in_cluster = [node for node, c in cluster_assignments.items() if c == cid]
#         for node in nodes_in_cluster:
#             x_raw, y_raw = pos[node]
#             x, y = normalize_position(x_raw, y_raw)
#             width = G.nodes[node].get('width', 0.1)
#             height = G.nodes[node].get('height', 0.1)

#             # 크기 정규화 스케일링
#             scaled_width = width * 0.01
#             scaled_height = height * 0.01

#             rect = Rectangle(
#                 (x - scaled_width / 2, y - scaled_height / 2),
#                 scaled_width, scaled_height,
#                 facecolor=color_map[cid],
#                 edgecolor='black',
#                 linewidth=0.5,
#                 alpha=0.8
#             )
#             ax.add_patch(rect)

#             if show_labels:
#                 ax.text(x, y, node, ha='center', va='center', fontsize=6)

#     # 3. 비클러스터 노드
#     none_nodes = [node for node, c in cluster_assignments.items() if c is None]
#     for node in none_nodes:
#         x_raw, y_raw = pos[node]
#         x, y = normalize_position(x_raw, y_raw)
#         width = G.nodes[node].get('width', 0.1)
#         height = G.nodes[node].get('height', 0.1)
#         scaled_width = width * 0.05
#         scaled_height = height * 0.05

#         rect = Rectangle(
#             (x - scaled_width / 2, y - scaled_height / 2),
#             scaled_width, scaled_height,
#             facecolor='lightgray',
#             edgecolor='black',
#             linewidth=0.5,
#             alpha=0.5
#         )
#         ax.add_patch(rect)

#         if show_labels:
#             ax.text(x, y, node, ha='center', va='center', fontsize=6)

#     # 4. 엣지 시각화
#     edge_weights = [data.get('weight', 1.0) for _, _, data in G.edges(data=True)]
#     edge_widths = [max(0.5, min(3.0, w * 1.0)) for w in edge_weights]
#     nx.draw_networkx_edges(G, pos, ax=ax, width=edge_widths, alpha=0.1)

#     # 5. 범례
#     if show_legend:
#         handles = [
#             mpatches.Patch(color=color_map[cid], label=f"Cluster {cid} (n={len([n for n in cluster_assignments if cluster_assignments[n] == cid])})")
#             for cid in sorted(all_clusters)
#         ]
#         if none_nodes:
#             handles.append(mpatches.Patch(color='lightgray', label=f'Unclustered (n={len(none_nodes)})'))
#         ax.legend(handles=handles, loc='best')

#     ax.set_title("Network Cluster Visualization")
#     ax.set_axis_off()
#     plt.tight_layout()
#     plt.show()

def draw_directed_edges_with_arrows(ax, G, pos, node_size_scale=0.03):
    for u, v, data in G.edges(data=True):
        if u not in pos or v not in pos:
            continue

        x1, y1 = pos[u]
        x2, y2 = pos[v]

        # 방향 벡터 계산
        dx = x2 - x1
        dy = y2 - y1
        norm = (dx ** 2 + dy ** 2) ** 0.5
        if norm == 0:
            continue  # 같은 위치면 그리지 않음

        # 노드 외곽에서 시작/종료하도록 위치 조정
        offset = node_size_scale  # 0.03 정도가 적당
        start_x = x1 + dx / norm * offset
        start_y = y1 + dy / norm * offset
        end_x = x2 - dx / norm * offset
        end_y = y2 - dy / norm * offset

        # 엣지 굵기/색상/화살표 크기
        weight = data.get('weight', 1.0)
        linewidth = max(0.5, min(2.5, weight * 0.5))

        arrow = FancyArrowPatch(
            (start_x, start_y), (end_x, end_y),
            connectionstyle="arc3,rad=0.05",  # 곡선 약간
            arrowstyle='->',
            mutation_scale=12,
            color='#4D4D4D',
            alpha=0.4,
            linewidth=linewidth
        )
        ax.add_patch(arrow)

def visualize_clusters_from_result(
    result: dict,
    layout: str = "spring",
    scc: bool = True,
    show_labels: bool = False,
    show_legend: bool = True,
    figsize = (12, 8),
    save_path = None,
    title_set = None
):
    G = result['graph']
    cluster_assignments = result['cluster_assignments']

    if scc:
        nodes_to_draw = [n for n in G.nodes() if n in cluster_assignments and cluster_assignments[n] is not None]
        G = G.subgraph(nodes_to_draw).copy()
        cluster_assignments = {n: c for n, c in cluster_assignments.items() if n in G.nodes()}
        
        print(f"[DEBUG] Filtered G: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        print(f"[DEBUG] Original G: {result['graph'].number_of_nodes()} nodes, {result['graph'].number_of_edges()} edges")

    # 레이아웃 좌표 생성
    if layout == 'kamada':
        pos = nx.kamada_kawai_layout(G)
    elif layout == 'circular':
        pos = nx.circular_layout(G)
    else:
        pos = nx.spring_layout(G, seed=42)

    # 1. 좌표 정규화
    x_vals = [p[0] for p in pos.values()]
    y_vals = [p[1] for p in pos.values()]
    x_min, x_max = min(x_vals), max(x_vals)
    y_min, y_max = min(y_vals), max(y_vals)
    x_range = x_max - x_min
    y_range = y_max - y_min

    def normalize_position(x, y):
        nx_ = (x - x_min) / x_range if x_range > 0 else 0.5
        ny_ = (y - y_min) / y_range if y_range > 0 else 0.5
        return nx_, ny_

    # 클러스터 색상 정의
    all_clusters = set(v for v in cluster_assignments.values() if v is not None)
    # color_map = {cid: plt.cm.tab20(i % 20) for i, cid in enumerate(sorted(all_clusters))}
    palette = sns.color_palette("Set2", len(all_clusters))  # "husl", "Set2", "muted", "pastel"
    color_map = {
        cid: palette[i]
        for i, cid in enumerate(sorted(all_clusters))
    }

    fig, ax = plt.subplots(figsize=figsize)
    
    min_node_size = 0.03  # 최소 크기 제한
    scale_factor = 0.07   # 적당한 확대 비율
    
    norm_pos = {node: normalize_position(*raw_pos) for node, raw_pos in pos.items()}
    draw_directed_edges_with_arrows(ax, G, norm_pos, node_size_scale=0.03)
    
    # 2. 클러스터 노드 시각화 (직사각형)
    for cid in sorted(all_clusters):
        nodes_in_cluster = [node for node, c in cluster_assignments.items() if c == cid]
        for node in nodes_in_cluster:
            x_raw, y_raw = pos[node]
            x, y = normalize_position(x_raw, y_raw)
            width = G.nodes[node].get('width', 0.1)
            height = G.nodes[node].get('height', 0.1)

            # 크기 정규화 스케일링
            # scaled_width = width * 0.1
            # scaled_height = height * 0.1
            scaled_width = max(math.log1p(width) * scale_factor, min_node_size)
            scaled_height = max(math.log1p(height) * scale_factor, min_node_size)

            rect = Rectangle(
                (x - scaled_width / 2, y - scaled_height / 2),
                scaled_width, scaled_height,
                facecolor=color_map[cid][:3],
                edgecolor='black',
                linewidth=0.5,
                alpha=1.0
            )
            ax.add_patch(rect)

            if show_labels:
                rect_x = x - scaled_width / 2
                rect_y = y - scaled_height / 2
                center_x = rect_x + scaled_width / 2
                center_y = rect_y + scaled_height / 2
                ax.text(center_x, center_y, node, ha='center', va='center', fontsize=10, fontweight='bold')

    # 3. 엣지 시각화
    edge_weights = [data.get('weight', 1.0) for _, _, data in G.edges(data=True)]
    # edge_widths = [max(0.5, min(3.0, w * 1.0)) for w in edge_weights]
    edge_widths = [max(0.5, min(15.0, math.log1p(w) * 12.0)) for w in edge_weights]
    
    # nx.draw_networkx_edges(G, pos, ax=ax, width=edge_widths, alpha=0.1)
    
    # 정규화된 좌표로 엣지를 그리기 위해 pos를 변환
    # norm_pos = {node: normalize_position(*raw_pos) for node, raw_pos in pos.items()}
    # nx.draw_networkx_edges(G, norm_pos, ax=ax, width=edge_widths, alpha=0.1)
    
    # norm_pos = {node: normalize_position(*raw_pos) for node, raw_pos in pos.items()}
    # draw_directed_edges_with_arrows(ax, G, norm_pos, node_size_scale=0.03)

    # 4. 범례
    if show_legend:
        handles = [
            mpatches.Patch(color=color_map[cid], label=f"Cluster {cid} (n={len([n for n in cluster_assignments if cluster_assignments[n] == cid])})")
            for cid in sorted(all_clusters)
        ]
        ax.legend(
            handles=handles,
            loc='best',
            fontsize=12,       
            handlelength=2.0,   
            labelspacing=0.4,   
            borderpad=0.6,
            frameon=True         
        )

    if title_set:
        title = title_set
    else:
        title = "Network Cluster Visualization"
        
    ax.set_title(f"{title}", fontsize=16, pad=50)
    ax.set_axis_off()
    plt.tight_layout()
    
    if save_path is not None:
        plt.savefig(f'{save_path}', dpi=300, bbox_inches='tight')
        
    plt.show()
    
def edge_stat_attr_maker(filename: str):
    
    result = pd.read_csv(f"{final_results_path}{filename}")
    
    edge_stat_collist = ['cause_abb', 'outcome_abb']
    edge_attr_collist = ['cause_abb', 'outcome_abb']

    for i in result.columns[2:]:

        spl = i.split('_')

        if spl[0] in ['edge']:
            edge_attr_collist.append(i)
        elif spl[0] not in ['cause', 'outcome']:
            edge_stat_collist.append(i)

    edge_stat = result[edge_stat_collist]
    edge_attr = result[edge_attr_collist]
    
    return edge_stat, edge_attr

def melting_edge_attr(edge_attr: pd.DataFrame):
    
    # 1. melt로 cause_abb, outcome_abb를 제외한 나머지를 long-form으로 변환
    id_vars = ['cause_abb', 'outcome_abb']
    value_vars = [col for col in edge_attr.columns if col not in id_vars]

    df_long = edge_attr.melt(id_vars=id_vars, value_vars=value_vars,
                               var_name='attribute_combo', value_name='count')

    # 2. count가 0인 항목은 제거
    df_long = df_long[df_long['count'] > 0].copy()

    # 3. attribute_1, value_1, attribute_2, value_2 파싱
    def parse_attributes(attr_str):
        parts = attr_str.replace('edge_', '').replace('_counts', '').split('_')
        if len(parts) == 2:
            # 예: sex_1
            return parts[0], parts[1], None, None
        elif len(parts) == 4:
            # 예: sex_1_age_0
            return parts[0], parts[1], parts[2], parts[3]
        else:
            raise ValueError(f"예상하지 못한 칼럼명 구조: {attr_str}")

    df_long[['attribute_1', 'value_1', 'attribute_2', 'value_2']] = df_long['attribute_combo'].apply(lambda x: pd.Series(parse_attributes(x)))

    # 4. 최종 컬럼 정리
    melt_edge_attr = df_long[['cause_abb', 'outcome_abb', 'attribute_1', 'value_1', 'attribute_2', 'value_2', 'count']].reset_index(drop=True)
    
    return melt_edge_attr

def transform_edge_attr(fu_attr_tuple: tuple):
    fu, edge_attr = fu_attr_tuple
    melted = melting_edge_attr(edge_attr).copy()
    melted['fu'] = fu
    melted = melted[['fu'] + [col for col in melted.columns if col != 'fu']]
    return melted

def melting_node_attr(node_info: pd.DataFrame):
    
    # 1. melt로 node_code를 제외한 나머지를 long-form으로 변환
    id_vars = ['node_code', 'width', 'height', 'Korean', 'English']
    value_vars = [col for col in node_info.columns if col not in id_vars]

    df_long = node_info.melt(id_vars=id_vars, value_vars=value_vars,
                             var_name='attribute_combo', value_name='count')

    # 2. count가 0인 항목은 제거
    df_long = df_long[df_long['count'] > 0].copy()

    # 3. attribute_1, value_1, attribute_2, value_2 파싱
    def parse_attributes(attr_str):
        parts = attr_str.replace('_counts', '').split('_')
        if len(parts) == 2:
            # 예: sex_1
            return parts[0], parts[1], None, None
        elif len(parts) == 4:
            # 예: sex_1_age_0
            return parts[0], parts[1], parts[2], parts[3]
        else:
            raise ValueError(f"예상하지 못한 칼럼명 구조: {attr_str}")

    df_long[['attribute_1', 'value_1', 'attribute_2', 'value_2']] = df_long['attribute_combo'].apply(lambda x: pd.Series(parse_attributes(x)))

    # 4. 최종 컬럼 정리
    melt_node_attr = df_long[['node_code', 'attribute_1', 'value_1', 'attribute_2', 'value_2', 'count']].reset_index(drop=True)
    
    return melt_node_attr

def DBver_melting_edge_attr(edge_attr: pd.DataFrame):
    # edge_pids_info_1~10.csv 를 바탕으로 그냥 edge_attr를 직접 생성

    # 1. melt로 cause_abb, outcome_abb를 제외한 나머지를 long-form으로 변환
    id_vars = ['cause_abb', 'outcome_abb']
    value_vars = [col for col in edge_attr.columns if col not in id_vars]

    df_long = edge_attr.melt(id_vars=id_vars, value_vars=value_vars,
                                var_name='attribute_combo', value_name='count')

    # 2. count가 0인 항목은 제거
    df_long = df_long[df_long['count'] > 0].copy()

    # 3. attribute_1, value_1, attribute_2, value_2 파싱
    def parse_attributes(attr_str):
        parts = attr_str.replace('_counts', '').split('_')
        if len(parts) == 2:
            # 예: sex_1
            return parts[0], parts[1], None, None
        elif len(parts) == 4:
            # 예: sex_1_age_0
            return parts[0], parts[1], parts[2], parts[3]
        else:
            raise ValueError(f"예상하지 못한 칼럼명 구조: {attr_str}")

    df_long[['attribute_1', 'value_1', 'attribute_2', 'value_2']] = df_long['attribute_combo'].apply(lambda x: pd.Series(parse_attributes(x)))

    # 4. 최종 컬럼 정리
    melt_edge_attr = df_long[['cause_abb', 'outcome_abb', 'attribute_1', 'value_1', 'attribute_2', 'value_2', 'count']].reset_index(drop=True)

    return melt_edge_attr

def DBver_transform_edge_attr(fu_attr_tuple: tuple):
    fu, edge_attr = fu_attr_tuple
    melted = DBver_melting_edge_attr(edge_attr).copy()
    melted['fu'] = fu
    melted = melted[['fu'] + [col for col in melted.columns if col != 'fu']]
    return melted

def plot_advanced_histogram(df, column, plot_type='hist', bins=30, 
                           figsize=(12, 8), title=None, 
                           show_stats=True, show_kde=True,
                           remove_outliers=False, lower_percentile=2.5, upper_percentile=97.5):
    """
    고급 히스토그램 함수 (여러 플롯 타입 지원 및 아웃라이어 제거 기능)
    
    Parameters:
    - df: DataFrame
    - column: 히스토그램을 그릴 컬럼명
    - plot_type: 'hist', 'kde', 'box', 'violin', 'all' 중 선택
    - bins: 히스토그램 구간 수
    - figsize: 그래프 크기
    - title: 그래프 제목
    - show_stats: 통계 정보 표시 여부
    - show_kde: KDE 곡선 표시 여부
    - remove_outliers: 아웃라이어 제거 여부 (기본값: False)
    - lower_percentile: 하위 제거할 퍼센트 (기본값: 2.5)
    - upper_percentile: 상위 제거할 퍼센트 (기본값: 97.5)
    """
    data = df[column].dropna()
    
    if len(data) == 0:
        print(f"컬럼 '{column}'에서 유효한 데이터를 찾을 수 없습니다.")
        return
    
    # 아웃라이어 제거 (요청된 경우)
    original_data = data.copy()
    if remove_outliers:
        lower_bound = data.quantile(lower_percentile / 100)
        upper_bound = data.quantile(upper_percentile / 100)
        data = data[(data >= lower_bound) & (data <= upper_bound)]
        
        print(f"아웃라이어 제거 적용:")
        print(f"  원본 데이터 포인트: {len(original_data):,}")
        print(f"  필터링된 데이터 포인트: {len(data):,}")
        print(f"  제거된 데이터: {len(original_data) - len(data):,} ({((len(original_data) - len(data))/len(original_data)*100):.1f}%)")
        print(f"  범위: {lower_percentile}% ~ {upper_percentile}% ({lower_bound:.4f} ~ {upper_bound:.4f})")
    
    if plot_type == 'all':
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        
        # 제목 (아웃라이어 정보 포함)
        title_suffix = f" (Outliers Removed: {lower_percentile}%-{upper_percentile}%)" if remove_outliers else ""
        fig.suptitle(f'{column} Distribution Analysis{title_suffix}', fontsize=16, fontweight='bold')
        
        # 히스토그램
        axes[0, 0].hist(data, bins=bins, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0, 0].set_title('Histogram')
        if show_kde:
            from scipy import stats
            kde = stats.gaussian_kde(data)
            x_range = np.linspace(data.min(), data.max(), 100)
            axes[0, 0].plot(x_range, kde(x_range) * len(data) * (data.max() - data.min()) / bins, 
                           color='red', linewidth=2, label='KDE')
            axes[0, 0].legend()
        
        # 박스플롯
        axes[0, 1].boxplot(data)
        axes[0, 1].set_title('Box Plot')
        
        # 바이올린 플롯
        axes[1, 0].violinplot(data)
        axes[1, 0].set_title('Violin Plot')
        
        # Q-Q 플롯
        from scipy import stats
        stats.probplot(data, dist="norm", plot=axes[1, 1])
        axes[1, 1].set_title('Q-Q Plot (Normality Test)')
        
    else:
        plt.figure(figsize=figsize)
        
        if plot_type == 'hist':
            plt.hist(data, bins=bins, alpha=0.7, color='skyblue', edgecolor='black')
            if show_kde:
                from scipy import stats
                kde = stats.gaussian_kde(data)
                x_range = np.linspace(data.min(), data.max(), 100)
                plt.plot(x_range, kde(x_range) * len(data) * (data.max() - data.min()) / bins, 
                        color='red', linewidth=2, label='KDE')
                plt.legend()
                
        elif plot_type == 'kde':
            sns.kdeplot(data, fill=True, alpha=0.7)
            
        elif plot_type == 'box':
            plt.boxplot(data)
            
        elif plot_type == 'violin':
            plt.violinplot(data)
        
        if title is None:
            title_suffix = f" (Outliers Removed: {lower_percentile}%-{upper_percentile}%)" if remove_outliers else ""
            title = f'{column} Distribution ({plot_type}){title_suffix}'
        plt.title(title, fontsize=14, fontweight='bold')
        plt.xlabel(column, fontsize=12)
        plt.ylabel('Frequency', fontsize=12)
    
    plt.tight_layout()
    plt.show()
    
    if show_stats:
        print(f"\n=== {column} 통계 정보 ===")
        print(data.describe())
        
        # 정규성 검정
        from scipy import stats
        stat, p_value = stats.shapiro(data) if len(data) <= 5000 else stats.normaltest(data)
        print(f"\n정규성 검정:")
        print(f"통계량: {stat:.4f}, p-value: {p_value:.4f}")
        print(f"정규분포 여부: {'예' if p_value > 0.05 else '아니오'} (α=0.05)")
        
        # 아웃라이어 정보 (제거된 경우)
        if remove_outliers:
            print(f"\n=== 아웃라이어 정보 ===")
            print(f"원본 범위: {original_data.min():.4f} ~ {original_data.max():.4f}")
            print(f"필터링된 범위: {data.min():.4f} ~ {data.max():.4f}")
            print(f"하위 {lower_percentile}% 미만 아웃라이어: {len(original_data[original_data < data.quantile(lower_percentile/100)]):,}")
            print(f"상위 {upper_percentile}% 초과 아웃라이어: {len(original_data[original_data > data.quantile(upper_percentile/100)]):,}")

def compare_two_datasets(df1, df2, column, name1='Dataset 1', name2='Dataset 2',
                        plot_type='all', bins=30, figsize=(16, 12), 
                        remove_outliers=False, lower_percentile=2.5, upper_percentile=97.5,
                        normalize=False, method='minmax', log_transform=False, log_method='log', show_kde=True):
    """
    두 데이터셋의 분포를 비교하는 함수 (4개 서브플롯)
    """
    
    def remove_outliers_percentile(data, lower_percentile=2.5, upper_percentile=97.5):
        """퍼센타일 기반 아웃라이어 제거 함수"""
        lower_bound = np.percentile(data, lower_percentile)
        upper_bound = np.percentile(data, upper_percentile)
        return data[(data >= lower_bound) & (data <= upper_bound)]
    
    def normalize_data(data, method='minmax'):
        """데이터 정규화 함수"""
        if method == 'minmax':
            return (data - data.min()) / (data.max() - data.min())
        elif method == 'zscore':
            return (data - data.mean()) / data.std()
        elif method == 'robust':
            median = data.median()
            mad = np.median(np.abs(data - median))
            return (data - median) / (1.4826 * mad)
        else:
            return data

    def log_transform_data(data, method = log_method):
        """로그 변환 함수"""
        if method == 'log':
            return np.log(data)
        elif method == 'log1p':  # log(1+x) - 0값 처리
            return np.log1p(data)
        elif method == 'log10':
            return np.log10(data + 1)  # 0값 처리
        else:
            return data
    
    # 데이터 전처리
    data1 = df1[column].dropna()
    data2 = df2[column].dropna()
    
    # 아웃라이어 제거
    if remove_outliers:
        data1 = remove_outliers_percentile(data1, lower_percentile, upper_percentile)
        data2 = remove_outliers_percentile(data2, lower_percentile, upper_percentile)
    
    # 로그 변환 (정규화 전에 적용)
    if log_transform:
        data1 = log_transform_data(data1)
        data2 = log_transform_data(data2)
        print(f"로그 변환 적용됨: {column}")

    # 정규화
    if normalize:
        data1 = normalize_data(data1, method=method)
        data2 = normalize_data(data2, method=method)
    
    # 4개 서브플롯 생성
    fig, axes = plt.subplots(2, 2, figsize=figsize)

    # 제목에 변환 정보 포함
    title_suffix = ""
    if log_transform:
        title_suffix += " (Log Transformed)"
    if normalize:
        title_suffix += f" (Normalized: {method})"
    
    fig.suptitle(f'{column} Distribution Comparison: {name1} vs {name2}{title_suffix}', 
                 fontsize=16, fontweight='bold')
    
    # 1. 오버레이 히스토그램 (좌상)
    ax1 = axes[0, 0]
    ax1.hist(data1, bins=bins, alpha=0.7, label=name1, color='skyblue', density=True)
    ax1.hist(data2, bins=bins, alpha=0.7, label=name2, color='lightcoral', density=True)
    if show_kde:
        from scipy import stats
        kde1 = stats.gaussian_kde(data1)
        kde2 = stats.gaussian_kde(data2)
        x_range = np.linspace(min(data1.min(), data2.min()), max(data1.max(), data2.max()), 200)
        ax1.plot(x_range, kde1(x_range), color='blue', linewidth=2, label=f'{name1} KDE')
        ax1.plot(x_range, kde2(x_range), color='red', linewidth=2, label=f'{name2} KDE')
    ax1.set_title('Overlay Histogram')
    ax1.set_xlabel(column)
    ax1.set_ylabel('Density')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. 박스플롯 비교 (우상)
    ax2 = axes[0, 1]
    box_data = [data1, data2]
    box_labels = [name1, name2]
    bp = ax2.boxplot(box_data, labels=box_labels, patch_artist=True)
    bp['boxes'][0].set_facecolor('skyblue')
    bp['boxes'][1].set_facecolor('lightcoral')
    ax2.set_title('Box Plot Comparison')
    ax2.set_ylabel(column)
    ax2.grid(True, alpha=0.3)
    
    # 3. 바이올린 플롯 비교 (좌하)
    ax3 = axes[1, 0]
    parts = ax3.violinplot([data1, data2], positions=[1, 2], showmeans=True, showmedians=True)
    parts['bodies'][0].set_facecolor('skyblue')
    parts['bodies'][1].set_facecolor('lightcoral')
    ax3.set_xticks([1, 2])
    ax3.set_xticklabels([name1, name2])
    ax3.set_title('Violin Plot Comparison')
    ax3.set_ylabel(column)
    ax3.grid(True, alpha=0.3)
    
    # 4. Q-Q 플롯 비교 (우하) - 두 데이터셋을 명확히 구분
    ax4 = axes[1, 1]
    from scipy import stats
    
    # 두 데이터셋의 Q-Q 플롯을 수동으로 생성
    sorted_data1 = np.sort(data1)
    sorted_data2 = np.sort(data2)
    
    # 이론적 분위수 계산
    n1 = len(sorted_data1)
    n2 = len(sorted_data2)
    theoretical_quantiles1 = stats.norm.ppf(np.linspace(0.01, 0.99, n1))
    theoretical_quantiles2 = stats.norm.ppf(np.linspace(0.01, 0.99, n2))
    
    # 두 데이터셋을 같은 축에 표시
    ax4.scatter(theoretical_quantiles1, sorted_data1, alpha=0.6, color='blue', s=2, label=name1)
    ax4.scatter(theoretical_quantiles2, sorted_data2, alpha=0.6, color='red', s=2, label=name2)
    
    # 정규분포 기준선 추가
    x_min = min(theoretical_quantiles1.min(), theoretical_quantiles2.min())
    x_max = max(theoretical_quantiles1.max(), theoretical_quantiles2.max())
    y_min = min(sorted_data1.min(), sorted_data2.min())
    y_max = max(sorted_data1.max(), sorted_data2.max())
    
    # 대각선 (정규분포 기준선)
    ax4.plot([x_min, x_max], [y_min, y_max], 'k--', alpha=0.8, linewidth=2, label='Normal Distribution')
    
    ax4.set_title('Q-Q Plot Comparison')
    ax4.set_xlabel('Theoretical quantiles')
    ax4.set_ylabel('Ordered Values')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # 통계 정보 표시 (위치 수정)
    # if show_stats:
    #     stats_text = f"""
    #     {name1}: n={len(data1):,}, mean={data1.mean():.3f}, std={data1.std():.3f}
    #     {name2}: n={len(data2):,}, mean={data2.mean():.3f}, std={data2.std():.3f}
    #     """
    #     # 텍스트 위치를 더 아래쪽으로 이동 (바이올린 플롯과 겹치지 않도록)
    #     fig.text(0.02, 0.01, stats_text, fontsize=10, verticalalignment='bottom')
    
    plt.tight_layout()
    # plt.show() 제거 - 호출하는 곳에서 처리
    
    return fig