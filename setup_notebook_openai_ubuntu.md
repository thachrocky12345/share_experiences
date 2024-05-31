## Table of contents

- Install Requirements
- Install openAI
- Troubleshooting

## Install Requirements

This module requires the following modules:
- Install Python 9.
  You can try:
  ```
  sudo apt update
  sudo apt install python3.9

  ```
- If it is not working. You try:
  ```
  
  sudo add-apt-repository ppa:deadsnakes/ppa
  sudo apt update
  sudo apt install build-essential libzmq3-dev

  sudo apt install python3.9
  sudo apt install python3.9-venv python3.9-distutils
  curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
  python3.9 get-pip.py

  sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 1
  sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 2

  sudo update-alternatives --config python3

  python3 --version

  deactivate
  rm -rf myenv


  python3.9 -m venv myenv
  source myenv/bin/activate

  pip install notebook
  pip install openai
  sudo apt install libatlas-base-dev




  
  ```
  For issues like pyzmq try:
  ```
  sudo apt update
  sudo apt install build-essential libzmq3-dev
  pip install --upgrade pip setuptools wheel
  pip cache purge
  pip install pyzmq --only-binary :all:
  pip install notebook

  ```
## Troubleshoots

- star jupyter:
  ```
  jupyter notebook

  ```
  or with setting:
  ```
  vi ~/.ipython/profile_nbserver/ipython_nbconvert_config.py
  c.NotebookApp.ip = '0.0.0.0'
  c.NotebookApp.port = 8888
  c.NotebookApp.open_browser = False

  ```
  Then
  ```
  jupyter notebook password
  
  jupyter notebook --ip 0.0.0.0 --port 8888
  ```
- test openAI
  ```
  import openai
  
  
  openai.api_key = 'your-api-key'
  
  
  response = openai.Completion.create(
    engine="text-davinci-003",
    prompt="Say this is a test",
    max_tokens=5
  )
  
  print(response.choices[0].text.strip())
  ```
