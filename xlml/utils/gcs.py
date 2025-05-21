import subprocess
import os
from airflow.decorators import task

def run_shell_command(command):
    """
    Runs a shell command and captures its output (stdout and stderr).

    Args:
        command (str): The shell command to execute.

    Returns:
        tuple: A tuple containing the standard output (stdout) and standard error (stderr) as strings.
               Returns (None, None) if the command fails to execute.
    """
    try:
        # Use subprocess.run for executing commands.
        process = subprocess.run(
            command,
            shell=True,  # IMPORTANT: Set shell=True to execute complex commands.
            capture_output=True,  # Capture stdout and stderr.
            text=True,  # Return output as strings, not bytes.
            check=True,  # Raise an exception if the command fails.
        )
        # If the command executed successfully, return the output.
        return process.stdout, process.stderr

    except subprocess.CalledProcessError as e:
        # Handle errors (e.g., command not found, non-zero exit code).
        print(f"Error running command: {e}")
        print(f"Return code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return None, None
    except FileNotFoundError as e:
        print(f"Error: Command not found: {e}")
        return None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None

@task.sensor(poke_interval=120, timeout=1200)    
def check_gcs_files(gcs_location):
    """
    usage of the run_shell_command function to get gcs bucket checkpoint files.
    """
    # Example 1: List files in the current directory.
    stdout, stderr = run_shell_command(f"gcloud storage ls {gcs_location}")
    if stdout:
        files = stdout.strip().split("\n")
        if len(files) > 0:
            return True
    if stderr:
        raise(stderr)
    return False
