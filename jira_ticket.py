import requests
import json

# JIRA server details
JIRA_SERVER = ''
JIRA_API_ENDPOINT = f'{JIRA_SERVER}/rest/api/2/issue'

# JIRA Personal Access Token (PAT)
JIRA_PAT = ""  # Replace with your actual PAT

# Issue details for Task
task_data = {
  "fields": {
    "project": {
      "key": "TES"
    },
    "summary": "Task Summary",
    "description": "Task Description",
    "issuetype": {
      "name": "Task"
    },
    "priority": {"name": "High"},
    "assignee": {"name": "thachbui"}
  }
}

def create_issue(issue_data):
    """
    return {'id': '10004', 'key': 'TES-5', 'self': 'http://host_url/rest/api/2/issue/10004'
    """
  headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {JIRA_PAT}"  # Bearer token for authorization
  }

  response = requests.post(
      JIRA_API_ENDPOINT,
      headers=headers,
      json=issue_data
  )

  if response.status_code == 201:
      print("Issue created successfully:", response.json())
  else:
      print("Failed to create issue:", response.status_code, response.text)

# Create Task
create_issue(task_data)
