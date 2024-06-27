import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Set your Slack API token here
# SLACK_API_TOKEN = 'your-slack-api-token'  # Replace with your actual token
SLACK_API_TOKEN = 'TOKEN'  # Replace with your actual token 

# Initialize the Slack client
client = WebClient(token=SLACK_API_TOKEN)

# Function to get the last message from a Slack channel
def get_last_message(channel_id):
    try:
        response = client.conversations_history(
            channel=channel_id,
            limit=5  # Get only the last message
        )
        print(response)
        messages = response['messages']
        if messages:
            last_message = messages[0]
            print(f"Last message in {channel_id}: {last_message['text']}")
        else:
            print(f"No messages found in {channel_id}")
    except SlackApiError as e:
        print(f"Error fetching messages: {e.response['error']}")
        
# Function to send a message to a Slack channel
def send_message(channel, text):
    try:
        response = client.chat_postMessage(
            channel=channel,
            text=text
        )
        print(f"Message sent to {channel}: {response['message']['text']}")
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")

def send_block_message(channel_id, blocks):
    try:
        response = client.chat_postMessage(
            channel=channel_id,
            blocks=blocks
        )
        print(f"Message sent to {channel_id}: {response['ts']}")
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")

def send_slack_message(snippet):
    permalink = "Slack Error"
    channel = "nv-test"

    try:
        client = WebClient(get_parameter("SLACK_TOKEN"))
        snippet_content = json.dumps(snippet, indent=4) if isinstance(snippet, dict) else str(snippet)

        response = client.files_upload(
            content=snippet_content, filename=f"{uuid.uuid4()}.json", initial_comment=text
        )
        permalink = response.get("file").get("permalink")
        client.chat_postMessage(
            channel=channel,
            text=f':rotating_light: DLAP-API error: {permalink}'
        )
        log.info(f"Sent {text} to {channel} and got response {response}")
    except Exception as error:

        log.error(
            f"An error occurred while sending message to slack channel {channel}. {str(error)}",
            exc_info=True,
        )
        traceback.print_exc()
       

    return permalink
	
# Usage example
if __name__ == "__main__":
    channel = '#general'  # Replace with your channel ID or name
    text = "test"
    blocks = [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "Test block with user https://gmatches.com select"
			},
			"accessory": {
				"type": "users_select",
				"placeholder": {
					"type": "plain_text",
					"text": "Select a user",
					"emoji": True
				},
				"action_id": "users_select-action"
			}
		}
	]
    send_block_message(channel, blocks)
    
    # for channel_id in ('C079AGDBEUT','C079D2ZUNSY'):  # Replace with your channel ID
    
    #     get_last_message(channel_id)
