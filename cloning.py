import git
#function to clone the git repository
def get():
    repo_url = "https://github.com/PhonePe/pulse.git" 
    destination_folder = r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse Data Visualization and Exploration A User-Friendly Tool Using Streamlit and Plotly/pulse'
    try:
        repo = git.Repo.clone_from(repo_url, destination_folder)
        print("Repository cloned successfully!")
    except Exception as e:
        print("already cloned")

if __name__ == "__main__":
    get()
    
