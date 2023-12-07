from flask import Flask
from flask import request
from flask import jsonify
import requests
import os
import json
app = Flask(__name__)

def get_api_key() -> str:
    secret = os.environ.get("COMPUTE_API_KEY")
    if secret:
        return secret
    else:
        #local testing
        with open('.key') as f:
            return f.read()
      
@app.route("/")
def hello():
    return "Add workers to the Spark cluster with a POST request to add"

@app.route("/test")
def test():
    #return "Test" # testing 
    return(get_api_key())

@app.route("/add",methods=['GET','POST'])
def add():
  if request.method=='GET':
    return "Use post to add" # replace with form template
  else:
    token=get_api_key()
    ret = addWorker(token,request.form['num'])
    return ret
  
@app.route("/delete",methods=['GET','POST'])
def delete():
  if request.method=='GET':
    return "Use post to delete"
  else:
    token=get_api_key()
    ret = deleteWorker(token,request.form['num'])
    return ret
  
@app.route("/get_active_vms")
def get_active_vms():
    token = get_api_key()
    vm_names = get_active_vm_names(token)

    return jsonify({"active_vms": vm_names})

def get_active_vm_names(token):
    url = 'https://www.googleapis.com/compute/v1/projects/spark-407315/zones/europe-west2-c/instances'
    headers = {"Authorization": "Bearer " + token}
    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        resp_json = resp.json()
        vm_names = [x['name'] for x in resp_json['items']]
        return vm_names
    else:
        print(resp.content)
        return "Error\n" + resp.content.decode('utf-8')


def addWorker(token, num):
    with open('payload.json') as p:
      tdata=json.load(p)
    tdata['name']='slave'+str(num)
    data=json.dumps(tdata)
    url='https://www.googleapis.com/compute/v1/projects/spark-407315/zones/europe-west2-c/instances'
    headers={"Authorization": "Bearer "+token}
    resp=requests.post(url,headers=headers, data=data)
    if resp.status_code==200:     
      return "Done"
    else:
      print(resp.content)
      return "Error\n"+resp.content.decode('utf-8') + '\n\n\n'+data
    
def deleteWorker(token, num):
    url='https://www.googleapis.com/compute/v1/projects/spark-407315/zones/europe-west2-c/instances/slave'+str(num)
    headers={"Authorization": "Bearer "+token}
    resp=requests.delete(url,headers=headers)
    if resp.status_code==200:     
      return "Done"
    else:
      print(resp.content)
      return "Error\n"+resp.content.decode('utf-8')



if __name__ == "__main__":
    app.run(host='0.0.0.0',port='8080')
