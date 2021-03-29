from flask import Flask, request, url_for, flash, redirect, jsonify
from datetime import datetime as DT, timedelta
from werkzeug.exceptions import abort
import json
import sqlite3

app = Flask(__name__)



connection_couriers = sqlite3.connect('couriers.db')
connection_orders = sqlite3.connect('orders.db')

cur_posts = connection_couriers.cursor()
cur_orders = connection_orders.cursor()
#cur.execute("""DROP TABLE IF EXISTS posts""")
cur_posts.execute("""CREATE TABLE IF NOT EXISTS couriers(
   courier_id INT PRIMARY KEY,
   courier_type TEXT,
   regions json,
   working_hours json,
   orders_list json,
   assign_time TEXT,
   done json);
""")

cur_orders.execute("""CREATE TABLE IF NOT EXISTS orders(
   order_id INT PRIMARY KEY,
   weight REAL,
   region INT,
   delivery_hours json,
   is_active INT
    );
""")


connection_couriers.commit()
connection_orders.commit()


def get_db_connection_couriers():
    conn = sqlite3.connect('couriers.db')
    conn.row_factory = sqlite3.Row

    return conn

def get_db_connection_orders():
    conn = sqlite3.connect('orders.db')
    conn.row_factory = sqlite3.Row

    return conn


def get_courier(courier_id):
    conn = get_db_connection_couriers()
    courier = conn.execute('SELECT * FROM couriers WHERE courier_id = ?',
                        (courier_id,)).fetchone()
    conn.close()
    if courier is None:
        abort(404)
    return courier

def get_order(order_id):
    conn = get_db_connection_orders()
    order = conn.execute('SELECT * FROM orders WHERE order_id = ?',
                        (order_id,)).fetchone()
    conn.close()
    if order is None:
        abort(404)
    return order

##Важно
@app.route('/couriers', methods=['POST'])
def couriers():
    if request.method == 'POST':
        incorr = []
        corr = []
        dt_fmt = '%d.%m.%Y %H:%M'
        items = request.get_json()['data']
        conn = get_db_connection_couriers()
        for i in items:
            try:
                if ('courier_type' not in i) or ('regions' not in i) or ('working_hours' not in i) or (i['courier_type'] == None) or (i['regions'] == None) or (i['working_hours'] == None):
                    incorr.append({'id' : i['courier_id']})
                    continue
                if (i['courier_type'] != 'foot' and i['courier_type'] != 'car' and i['courier_type'] != 'bike'):
                    incorr.append({'id' : i['courier_id']})
                    continue
                k = 0
                for j in i['regions']:
                    if str(type(j)) != "<class 'int'>":
                        incorr.append({'id' : i['courier_id']})
                        k = 1
                if k == 1:
                    continue
                k = 0
                for j in i['working_hours']:
                    try:
                        date_s = j.split('-')
                        if len(date_s) != 2:
                            raise Exception()
                        x1 = DT.strptime('01.01.2000 ' + date_s[0], dt_fmt)
                        x2 = DT.strptime('01.01.2000 ' + date_s[1], dt_fmt)
                        if (x2<x1):
                            raise Exception()
                    except:
                        k = 1
                if k == 1:
                     incorr.append({'id' : i['courier_id']})
                     continue
                try:
                    conn.execute('INSERT INTO couriers (courier_id, courier_type, regions, working_hours, orders_list, done) VALUES (?, ?, ?, ?, ?, ?)',
                         (i['courier_id'], i['courier_type'], json.dumps(i['regions']), json.dumps(i['working_hours']), json.dumps([]), json.dumps([])))
                except:
                    incorr.append({'id' : i['courier_id']})
                    continue
            except:
                continue
            corr.append({'id' : i['courier_id']})
        if len(incorr) == 0:
            conn.commit()

            print(conn.execute('SELECT * FROM couriers').fetchall())
            conn.close()
            return jsonify({"couriers" : corr}), 201
        else:
            conn.commit()
            conn.close()
                
            return jsonify({"validation_error" : {"couriers" : incorr}}), 400
        return a


@app.route('/orders', methods=['POST'])
def orders():
    if request.method == 'POST':
        incorr = []
        dt_fmt = '%d.%m.%Y %H:%M'
        corr = []
        items = request.get_json()['data']
        conn = get_db_connection_orders()
        for i in items:
            if  'weight' not in i or 'region' not in i or 'delivery_hours' not in i or i['weight'] == None or i['region'] == None or i['delivery_hours'] == None:
                incorr.append({'id' : i['order_id']})
                continue
            if (str(type(i['weight'])) != "<class 'int'>" and str(type(i['weight'])) != "<class 'float'>") or (float(i['weight']) > 50 or float(i['weight']) < 0.01):
                incorr.append({'id' : i['order_id']})
                continue
            if str(type(i['region'])) != "<class 'int'>":
                incorr.append({'id' : i['order_id']})
                continue
            k = 0
            for j in i['delivery_hours']:
                try:
                   date_s = j.split('-')
                   if len(date_s) != 2:
                       raise Exception()
                   x1 = DT.strptime('01.01.2000 ' + date_s[0], dt_fmt)
                   x2 = DT.strptime('01.01.2000 ' + date_s[1], dt_fmt)
                   if (x2<x1):
                       raise Exception()
                except:
                    incorr.append({'id' : i['order_id']})
                    k = 1
                    break;
            if k == 1:
                continue
            try:
                conn.execute('INSERT INTO orders (order_id, weight, region, delivery_hours, is_active) VALUES (?, ?, ?, ?, ?)',
                         (i['order_id'], i['weight'], i['region'], json.dumps(i['delivery_hours']), 0))
            except:
                incorr.append({'id' : i['order_id']})
                continue
            corr.append({'id' : i['order_id']})
        if len(incorr) == 0:
            conn.commit()
            conn.close()
            return jsonify({"orders" : corr}), 200
        else:
            conn.commit()
            conn.close()
            return jsonify({"validation_error" : {"orders" : incorr}}), 400

@app.route('/couriers/<int:courier_id>', methods=['PATCH'])
def id_to_change(courier_id):
    if request.method == 'PATCH':
        try:
            courier = get_courier(courier_id)
            items = request.get_json()
        except:
            return "", 400
        conn = get_db_connection_couriers()
        conn_ord = get_db_connection_orders()
        if len(items.keys()) == 0:
            return "", 400

        for i in items.keys():
            conn.execute('UPDATE couriers SET {} = ? WHERE courier_id = ?'.format(i), 
                     (json.dumps(items[i]), courier_id))
        conn.commit()
        courier = get_courier(courier_id)
        if courier['courier_type'] == 'foot':
            weight = 10
        if courier['courier_type'] == 'bike':
            weight = 15
        if courier['courier_type'] == 'car':
            weight = 50
        dates = json.loads(courier['working_hours'])
        orders_list = json.loads(courier['orders_list'])
        
        if len(orders_list) != 0:
            dt_fmt = '%d.%m.%Y %H:%M'
            t = []
            c = []
            orders = []
            for i in orders_list:
                orders.append(get_order(i))
            for i in items.keys():
                order_list = []
                if i == 'working_hours':  
                    for j in orders:    
                        k = 0
                        dates_order = json.loads(j['delivery_hours'])
                        for date_cor in dates:
                            date_s = date_cor.split('-')
                            x1 = DT.strptime('01.01.2000 ' + date_s[0], dt_fmt)
                            x2 = DT.strptime('01.01.2000 ' + date_s[1], dt_fmt)
                            for date_ord in dates_order:
                                date_o = date_ord.split('-')
                                y1 = DT.strptime('01.01.2000 ' + date_o[0], dt_fmt)
                                y2 = DT.strptime('01.01.2000 ' + date_o[1], dt_fmt)
                                print(x1, x2, y1, y2)
                                if (x2 > y2 and y2 > x1) or (x2 > y1 and y1 > x1) or (y2 > x1 and x1 > y1) or (y2 > x2 and x2 > y1):
                                    k = 1
                        if k == 0:
                            conn_ord.execute('UPDATE orders SET is_active = ? WHERE order_id = ?', (0, j['order_id']))
                        else:
                            order_list.append(j['order_id'])
                    conn.execute('UPDATE couriers SET orders_list = ? WHERE courier_id = ?', 
                                (json.dumps(order_list), courier_id))
                if i == 'weight':
                    for j in orders:
                        if weight < j['weight']:
                            conn_ord.execute('UPDATE orders SET is_active = ? WHERE order_id = ?', (0, j['order_id']))
                        else:
                            order_list.append(j['order_id'])
                    conn.execute('UPDATE couriers SET orders_list = ? WHERE courier_id = ?', 
                                (json.dumps(order_list), courier_id))
                if i == 'regions':
                    for j in orders:
                        if j['regions'] not in set(json.loads(courier['regions'])):
                            conn_ord.execute('UPDATE orders SET is_active = ? WHERE order_id = ?', (0, j['order_id']))
                        else:
                            order_list.append(j['order_id'])
                    conn.execute('UPDATE couriers SET orders_list = ? WHERE courier_id = ?', 
                                (json.dumps(order_list), courier_id))

        conn.commit()
        conn.close()
        conn_ord.commit()
        conn_ord.close()
        courier = get_courier(courier_id)

        ans = dict()
        ans['id_courier'] = courier['courier_id']
        ans['courier_type'] = courier['courier_type']
        ans['regions'] = json.loads(courier['regions'])
        ans['working_hours'] = json.loads(courier['working_hours'])
        
        return jsonify(ans), 200
        

@app.route('/orders/assign', methods=['POST'])
def assign():
    if request.method == 'POST':
        courier_id = request.get_json()['courier_id']
        
        try:    
            courier = get_courier(courier_id)
        except:
            return "", 400

        
        if json.loads(courier['orders_list']) != []:
            a = json.loads(courier['orders_list'])
            ans = []
            for i in a:
                ans.append({'id':i})
            return jsonify({"orders":ans, 'assign_time' : courier['assign_time']}), 200

        
        conn1 = get_db_connection_orders()
        conn2 = get_db_connection_couriers()
        orders = conn1.execute('SELECT * FROM orders').fetchall()
        if courier['courier_type'] == 'foot':
            weight = 10
        if courier['courier_type'] == 'bike':
            weight = 15
        if courier['courier_type'] == 'car':
            weight = 50
        dates = json.loads(courier['working_hours'])
        
        dt_fmt = '%d.%m.%Y %H:%M'
        t = []
        c = []
        for i in orders:
            k = 0
            if i['region'] in set(json.loads(courier['regions'])):
                if i['weight'] <= weight:
                    dates_order = json.loads(i['delivery_hours'])
                    for date_cor in dates:
                        date_s = date_cor.split('-')
                        x1 = DT.strptime('01.01.2000 ' + date_s[0], dt_fmt)
                        x2 = DT.strptime('01.01.2000 ' + date_s[1], dt_fmt)
                        for date_ord in dates_order:
                            date_o = date_ord.split('-')
                            y1 = DT.strptime('01.01.2000 ' + date_o[0], dt_fmt)
                            y2 = DT.strptime('01.01.2000 ' + date_o[1], dt_fmt)
                            print(x1, x2, y1, y2)
                            if ((x2 > y2 and y2 > x1) or (x2 > y1 and y1 > x1) or (y2 > x1 and x1 > y1) or (y2 > x2 and x2 > y1)) and i['is_active'] == 0:
                                k = 1
                    if k == 1:
                        conn1.execute('UPDATE orders SET is_active = ? WHERE order_id = ?', (1, i['order_id']))
                        c.append({
                            'id':i['order_id']})
                        order_list = json.loads((conn2.execute('SELECT * FROM couriers WHERE courier_id = ?',
                                                  (courier_id,)).fetchone())['orders_list'])
                        order_list.append(i['order_id'])
                        conn2.execute('UPDATE couriers SET orders_list = ? WHERE courier_id = ?', 
                             (json.dumps(order_list), courier_id))
                        
        dt = DT.utcnow()
        dt_fmtt = '%d.%m.%Y %H:%M:%S'
        res = (dt + timedelta(hours=3)).isoformat('T') + "Z"
        k = {'assign_time' : res}
        conn2.execute('UPDATE couriers SET assign_time = ? WHERE courier_id = ?', 
                             (res, courier_id))

        conn1.commit()
        conn1.close()
        conn2.commit()
        conn2.close()
        if c != []:
            return jsonify({"orders":c, 'assign_time' : res}), 200
        else:
             return jsonify({"orders":c}), 200

         #orders list nepravilno
@app.route('/orders/complete', methods=['POST'])
def complete():
    try:
        courier_id = request.get_json()['courier_id']
        order_id = request.get_json()['order_id']
    except:
        return "", 400
    try:
        courier = get_courier(courier_id)
        order = get_order(order_id)
    except:
        return "", 400

    if courier['done'] != []:
        if order_id in set(json.loads(courier['done'])):
            return jsonify({"order_id":order_id}), 200

    conn = get_db_connection_couriers()
    a = json.loads(courier['orders_list'])
    k = 0
    for i in a:
        if i == order_id:
            k = 1
            a.remove(order_id)
            conn.execute('UPDATE couriers SET orders_list = ? WHERE courier_id = ?', 
                             (json.dumps(a), courier_id))
            done = json.loads(courier['done'])
            done.append(order_id)
            conn.execute('UPDATE couriers SET done = ? WHERE courier_id = ?', 
                             (json.dumps(done), courier_id))
            conn.commit()
            conn.close()

    if k == 1:
        return jsonify({"order_id":order_id}), 200
    else:
        return "", 400
    
app.run(host='0.0.0.0', port=8080)
