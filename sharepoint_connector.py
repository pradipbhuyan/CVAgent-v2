import base64
import os
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
ALLOWED_CV_EXTENSIONS = {".pdf", ".docx", ".txt"}


class SharePointConnectorError(Exception):
    pass


def _get_access_token() -> str:
    tenant_id = os.getenv("MS_TENANT_ID")
    client_id = os.getenv("MS_CLIENT_ID")
    client_secret = os.getenv("MS_CLIENT_SECRET")

    if not tenant_id or not client_id or not client_secret:
        raise SharePointConnectorError(
            "Missing Microsoft Graph credentials. "
            "Set MS_TENANT_ID, MS_CLIENT_ID, and MS_CLIENT_SECRET."
        )

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }

    resp = requests.post(token_url, data=data, timeout=30)
    if not resp.ok:
        raise SharePointConnectorError(
            f"Token request failed [{resp.status_code}]: {resp.text}"
        )

    token = resp.json().get("access_token")
    if not token:
        raise SharePointConnectorError("Access token missing in token response.")

    return token


def _graph_get(path: str, token: str, params: Optional[dict] = None, raw: bool = False):
    url = path if path.startswith("http") else f"{GRAPH_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, headers=headers, params=params, timeout=60)
    if not resp.ok:
        raise SharePointConnectorError(
            f"Graph GET failed [{resp.status_code}]: {resp.text}"
        )

    return resp.content if raw else resp.json()


def _parse_sharepoint_site_url(site_url: str) -> Tuple[str, str]:
    parsed = urlparse(site_url)
    hostname = parsed.netloc
    site_path = parsed.path.rstrip("/")

    if not hostname or not site_path:
        raise SharePointConnectorError(f"Invalid SharePoint site URL: {site_url}")

    return hostname, site_path


def _get_site_by_url(site_url: str, token: str) -> dict:
    hostname, site_path = _parse_sharepoint_site_url(site_url)
    return _graph_get(f"/sites/{hostname}:{site_path}", token)


def _list_site_drives(site_id: str, token: str) -> List[dict]:
    data = _graph_get(f"/sites/{site_id}/drives", token)
    return data.get("value", [])


def _find_drive_by_name(drives: List[dict], library_name: str) -> dict:
    for drive in drives:
        if (drive.get("name") or "").strip().lower() == library_name.strip().lower():
            return drive

    available = [d.get("name", "?") for d in drives]
    raise SharePointConnectorError(
        f"Library '{library_name}' not found. Available libraries: {available}"
    )


def _list_folder_children_by_path(drive_id: str, folder_path: str, token: str) -> List[dict]:
    folder_path = folder_path.strip("/")

    if not folder_path:
        endpoint = f"/drives/{drive_id}/root/children"
    else:
        endpoint = f"/drives/{drive_id}/root:/{folder_path}:/children"

    data = _graph_get(endpoint, token)
    return data.get("value", [])


def _download_drive_item(drive_id: str, item_id: str, token: str) -> bytes:
    return _graph_get(f"/drives/{drive_id}/items/{item_id}/content", token, raw=True)


def _is_cv_filename(name: str) -> bool:
    lower = name.lower()
    if lower.startswith("~$"):
        return False
    return any(lower.endswith(ext) for ext in ALLOWED_CV_EXTENSIONS)


def _collect_cv_files_from_children(drive_id: str, children: List[dict], token: str) -> List[Dict[str, bytes]]:
    results = []

    for item in children:
        name = item.get("name", "")
        item_id = item.get("id")

        if "file" not in item or not item_id or not _is_cv_filename(name):
            continue

        content = _download_drive_item(drive_id, item_id, token)
        results.append({
            "name": name,
            "content": content,
        })

    return results


def _encode_share_url(shared_url: str) -> str:
    encoded = base64.urlsafe_b64encode(shared_url.encode("utf-8")).decode("utf-8")
    encoded = encoded.rstrip("=")
    return f"u!{encoded}"


def get_cv_files_from_sharepoint(site_url: str, folder_path: str, library_name: str = "Documents"):
    token = _get_access_token()
    site = _get_site_by_url(site_url, token)
    site_id = site["id"]
    drives = _list_site_drives(site_id, token)
    drive = _find_drive_by_name(drives, library_name)
    drive_id = drive["id"]
    children = _list_folder_children_by_path(drive_id, folder_path, token)
    return _collect_cv_files_from_children(drive_id, children, token)


def get_cv_files_from_onedrive(drive_id: str, folder_path: str):
    token = _get_access_token()
    children = _list_folder_children_by_path(drive_id, folder_path, token)
    return _collect_cv_files_from_children(drive_id, children, token)


def get_cv_files_from_onedrive_url(shared_url: str):
    token = _get_access_token()
    share_token = _encode_share_url(shared_url)

    item = _graph_get(f"/shares/{share_token}/driveItem", token)
    drive_id = item.get("parentReference", {}).get("driveId")
    item_id = item.get("id")

    if not drive_id or not item_id:
        raise SharePointConnectorError("Could not resolve shared OneDrive folder URL.")

    children_data = _graph_get(f"/drives/{drive_id}/items/{item_id}/children", token)
    children = children_data.get("value", [])
    return _collect_cv_files_from_children(drive_id, children, token)


def get_cv_files_from_sharepoint_url(folder_url: str):
    return get_cv_files_from_onedrive_url(folder_url)
