import requests


req = requests.get("http://api.github.com/repos/kestra-io/kestra")
gh_stars = req.json()["stargazers_count"]
print(gh_stars)
