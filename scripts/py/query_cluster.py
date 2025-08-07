from google.cloud import container_v1

def get_gke_cluster_details(project_id: str, location: str, cluster_name: str):
    """
    Retrieves and prints detailed information for a specific GKE cluster.

    Args:
        project_id: The Google Cloud project ID.
        location: The cluster's location (zone or region, e.g., 'us-central1-a' or 'us-central1').
        cluster_name: The name of the GKE cluster.
    """
    try:
        # Instantiate a client for the Container API
        client = container_v1.ClusterManagerClient()

        # Construct the full resource name for the cluster
        cluster_resource_name = f"projects/{project_id}/locations/{location}/clusters/{cluster_name}"
        print(cluster_resource_name)

        # Make the API call to get the cluster details
        request = container_v1.GetClusterRequest(name=cluster_resource_name)
        cluster = client.get_cluster(request=request)

        # Print the requested cluster information
        print("GKE Cluster Information:")
        print(f"  - Name: {cluster.name}")
        print(f"  - Project: {project_id}")
        print(f"  - Zone: {cluster.location}")
        print(f"  - Status: {cluster.status.name}")
        print(f"  - Cluster Definition Name: {cluster.self_link}") # The self-link often serves as a unique definition name
        
        # Check if an accelerator type is defined on the default node config
        accelerator_type = "N/A"
        accelerator_info = "N/A"
        initial_node_count = 0
        if cluster.node_config is not None and cluster.node_config.accelerators:
            accelerator_type = cluster.node_config.accelerators[0].accelerator_type
            accelerator_info = f"Type: {cluster.node_config.accelerators[0].accelerator_type}, " \
                               f"Count: {cluster.node_config.accelerators[0].accelerator_count}"
        
        # Note: The initial_node_count field is from the legacy default node pool
        initial_node_count = cluster.initial_node_count
        size_requested = initial_node_count
        exists = True # If the get_cluster call succeeds, the cluster exists
        
        print(f"  - Accelerator Type (Default Node): {accelerator_type}")
        print(f"  - Size Requested (Initial Node Count): {size_requested}")
        print(f"  - Exists: {exists}")
        
        print("\nNode Pool Details:")
        if cluster.node_pools:
            for node_pool in cluster.node_pools:
                print(f"\n  Node Pool Name: {node_pool.name}")
                print(f"    - Status: {node_pool.status.name}")
                print(f"    - Machine Type: {node_pool.config.machine_type}")
                print(f"    - Initial Node Count: {node_pool.initial_node_count}")
                
                # Extract accelerator info from the node pool config
                if node_pool.config.accelerators:
                    for accelerator in node_pool.config.accelerators:
                        print(f"    - Accelerator Info: Type={accelerator.accelerator_type}, Count={accelerator.accelerator_count}, Device_version={accelerator.gpu_driver_version}")
                else:
                    print("    - Accelerator Info: None")
        else:
            print("  No node pools found.")

    except Exception as e:
        print(f"An error occurred: {e}")
        
if __name__ == "__main__":
    # Replace with your actual project, location, and cluster name
    #my_project_id = "tpu-prod-env-large-adhoc"
    #my_cluster_location = "us-central2"  # e.g., "us-central1-a" or "us-central1"
    #my_cluster_name = "bodaborg-v6e-256-europe-west4-a"
    #my_project_id = "cloud-ml-auto-solutions"
    #my_cluster_location = "us-central2"  # e.g., "us-central1-a" or "us-central1"
    #my_cluster_name = "mas-v4-8"
    #my_project_id = "tpu-prod-env-multipod"
    #my_cluster_location = "europe-west4"  # e.g., "us-central1-a" or "us-central1"
    #my_cluster_name = "v5e-256-bodaborg-europe-west4"
    my_project_id = "tpu-prod-env-multipod"
    my_cluster_location = "us-central2"  # e.g., "us-central1-a" or "us-central1"
    my_cluster_name = "v4-16-maxtext"

#projects/tpu-prod-env-multipod/locations/us-central2/clusters/v4-16-maxtext
    # Call the function to get the cluster details
    get_gke_cluster_details(my_project_id, my_cluster_location, my_cluster_name)
