import requests
import streamlit as st


def get_graph_token():
    tenant_id = st.secrets["microsoft"]["tenant_id"]
    client_id = st.secrets["microsoft"]["client_id"]
    client_secret = st.secrets["microsoft"]["client_secret"]

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    response = requests.post(token_url, data=data, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


def get_graph_headers():
    token = get_graph_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
