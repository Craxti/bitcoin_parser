from mongo import Mongo
from flask import Flask, request, make_response, jsonify
import json

app = Flask(__name__, subdomain_matching=True)
db = Mongo('localhost', 27017)

@app.route("/GetAddressInfo", methods=['GET'])
async def getAddressInfo():
    if not "address" in request.args.keys(): return make_response(jsonify({'error': 'Undefined address'}), 400)
    if not "key" in request.args.keys(): return make_response(jsonify({"error": "Undefined key"}), 400)
    category = None if "category" in request.args.keys() else request.args.get("category")
    answer = await db.get_address_info(request.args.get("address"), category)
    return jsonify(answer)

@app.route("/GetCollectionStat", methods=['GET'])
async def getCollectionStat():
    if not "key" in request.args.keys(): return make_response(jsonify({"error": "Undefined key"}), 400)
    answer = await db.get_collection_stat()
    return jsonify(answer)

@app.route('/AddToAddress', methods=['POST'])
async def addToAddress():
    if not "key" in request.args.keys(): return make_response(jsonify({"error": "Undefined key"}), 400)
    data = json.loads(request.get_data())
    if not 'bitcoin_address' in data: return make_response(jsonify({"error": "Undefined bitcoin_address"}), 400)
    id = await db.add_to_address(data)
    return jsonify({"status": "Success", "id": id})

@app.route("/GetAddressStat", methods=['GET'])
async def getAddressStat():
    if not "key" in request.args.keys(): return make_response(jsonify({"error": "Undefined key"}), 400)
    if not "address" in request.args.keys(): return make_response(jsonify({'error': 'Undefined address'}), 400)
    answer = await db.get_collection_address(request.args.get("address"))
    return jsonify(answer)

@app.route("/GetAddressesList", methods=['GET'])
async def getAddressesList():
    if not "key" in request.args.keys(): return make_response(jsonify({"error": "Undefined key"}), 400)
    if not "category" in request.args.keys(): return make_response(jsonify({'error': 'Undefined category'}), 400)
    page = None if "page" not in request.args.keys() else request.args.get("page")
    q = False if "q" not in request.args.keys() else True
    answer = await db.get_address_category(request.args.get("category"), page, q)
    return jsonify(answer)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)