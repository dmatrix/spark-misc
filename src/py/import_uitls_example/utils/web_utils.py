import requests

def get_html(url):
    response = requests.get(url)
    return response.text

def get_json(url):
    response = requests.get(url)
    return response.json()

def get_text(url):
    response = requests.get(url)
    return response.text

def get_binary(url):
    response = requests.get(url)
    return response.content

def get_file(url):
    response = requests.get(url)
    return response.content
