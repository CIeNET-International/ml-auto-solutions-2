from google.cloud import bigquery, container_v1, storage
from google.api_core.exceptions import NotFound
import json
from datetime import datetime, timezone

# --- Step 1: Config ---
project_id = "cienet-cmcs"
dataset_id = "amy_xlml_poc_prod"
view_name = "cluster_view"

# --- Google Sheets Config ---
GSPREAD_INSERT_ENABLED = False
GSPREAD_SHEET_ID = '1FkyzvC0HGxFPGjIpCV4pWHB_QFWUOtLPIlDfnBNvt4Q'
GSPREAD_WORKSHEET_NAME = 'unhealthy_clusters'
GSPREAD_CREDS_PATH = 'cienet-cmcs-86d8fc4f484f.json'


# --- Step 2: Get rows from BQ view ---
def get_clusters_from_view():
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT project_name, cluster_name
        FROM `{project_id}.{dataset_id}.{view_name}`
    """
    rows = list(client.query(query).result())
    print(f"Fetched {len(rows)} rows from view.")
    return rows


# --- Step 3: List clusters from GKE API ---
def list_clusters_for_project(project_id):
    client = container_v1.ClusterManagerClient()
    parent = f"projects/{project_id}/locations/-"
    response = client.list_clusters(request={"parent": parent})
    return response.clusters if response and response.clusters else []


# --- Step 4: Get detailed cluster status ---
def get_cluster_status(project_id, location, cluster_name):
    client = container_v1.ClusterManagerClient()
    name = f"projects/{project_id}/locations/{location}/clusters/{cluster_name}"
    request = container_v1.GetClusterRequest(name=name)
    cluster = client.get_cluster(request=request)

    node_pools_info = []
    for np in cluster.node_pools:
        node_pools_info.append({
            "name": np.name,
            "status": container_v1.NodePool.Status(np.status).name if np.status else "UNKNOWN",
            "status_message": np.status_message or None,
            "version": np.version,
            "autoscaling_enabled": np.autoscaling.enabled if np.autoscaling else False,
            "initial_node_count": np.initial_node_count,
            "machine_type": np.config.machine_type if np.config else None,
            "disk_size_gb": np.config.disk_size_gb if np.config else None,
            "preemptible": np.config.preemptible if np.config else False,
        })

    return {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "region": location,
        "status": container_v1.Cluster.Status(cluster.status).name if cluster.status else "UNKNOWN",
        "status_message": cluster.status_message or None,
        "node_pools": node_pools_info,
    }

# --- Gspread function ---
def insert_gspread_rows(rows):
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_CREDS_PATH, scope)
        gspread_client = gspread.authorize(creds)
        sheet = gspread_client.open_by_key(GSPREAD_SHEET_ID)
        worksheet = sheet.worksheet(GSPREAD_WORKSHEET_NAME)

        print(f"Inserting {len(rows)} rows into Google Sheet...")
        # Appends new rows to the existing sheet.
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')
        print(f"Successfully appended rows to Google Sheet.")

    except Exception as e:
        print(f"An error occurred while writing to Google Sheet: {e}")


def main():
    start_date = datetime.now()
    rows = get_clusters_from_view()
    if not rows:
        print("No rows found in view.")
        return

    cluster_locs = {}
    projects = set(row.project_name for row in rows)

    print(f"Fetching clusters from {len(projects)} distinct projects...")

    for idx, project in enumerate(projects, 1):
        try:
            clusters = list_clusters_for_project(project)
            print(f"[{idx}/{len(projects)}] {project}: {len(clusters)} clusters")
            for c in clusters:
                key = f"{project}::{c.name}"
                cluster_locs[key] = c.location
        except Exception as e:
            print(f"Error listing clusters for {project}: {e}")


    result_rows = []
    gspread_rows_to_insert = []
    load_time = datetime.now(timezone.utc).isoformat()
    now_utc = datetime.now(timezone.utc).isoformat()

    for row in rows:
        proj = row.project_name
        cname = row.cluster_name
        op_region = None
        key = f"{proj}::{cname}"
        location = cluster_locs.get(key)

        cluster_status = "NOT EXIST"
        cluster_status_message = None
        node_pools_data = []

        if not location:
            # Matches fixed header: Issue Type, Project ID, Cluster Name, Cluster Status, Cluster Status Message, Node Pool Name, Node Pool Status, Node Pool Status Message, Appended at
            gspread_rows_to_insert.append([
                "Cluster", proj, cname, "NOT EXIST", "Cluster not found in GKE API.", "", "", "", now_utc
            ])
        else:
            try:
                info = get_cluster_status(proj, location, cname)
                cluster_status = info["status"]
                cluster_status_message = info["status_message"]
                node_pools_data = info["node_pools"]

                # Check cluster status for Gspread
                if cluster_status != "RUNNING":
                    gspread_rows_to_insert.append([
                        "Cluster", proj, cname, cluster_status, cluster_status_message, "", "", "", now_utc
                    ])

                # Check each node pool status for Gspread
                for np in node_pools_data:
                    if np["status"] != "RUNNING":
                        gspread_rows_to_insert.append([
                            "NodePool", proj, cname, cluster_status, cluster_status_message, np["name"], np["status"], np["status_message"], now_utc
                        ])

            except NotFound:
                cluster_status = "NOT EXIST"
                gspread_rows_to_insert.append([
                    "Cluster", proj, cname, "NOT EXIST", "Cluster not found in GKE API.", "", "", "", now_utc
                ])
            except Exception as e:
                print(f"Error fetching details for {proj}/{cname}: {e}")
                cluster_status = "ERROR"
                cluster_status_message = str(e)
                gspread_rows_to_insert.append([
                    "Cluster", proj, cname, cluster_status, cluster_status_message, "", "", "", now_utc
                ])

        result_rows.append({
            "project_id": proj,
            "cluster_name": cname,
            "region": location,
            "load_time": load_time,
            "status": cluster_status,
            "status_message": cluster_status_message,
            "node_pools": node_pools_data,
        })

    print(f'result:{result_rows}')
    print(f'errors:{gspread_rows_to_insert}')
    # Insert rows into Google Sheet
    if GSPREAD_INSERT_ENABLED and gspread_rows_to_insert:
        insert_gspread_rows(gspread_rows_to_insert)

    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')


# --- Run ---
if __name__ == "__main__":
    main()


