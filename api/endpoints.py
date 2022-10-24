from collections import Counter
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from flask import Blueprint, jsonify, request

from api.config import MONTH_ID as month_id
from api.query import *

inventory_bp = Blueprint('inventory', __name__)


def get_default_date(tgl_awal, tgl_akhir):
    if tgl_awal == None:
        tgl_awal = datetime.strptime((datetime.today() - relativedelta(months=1)).strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_awal = datetime.strptime(tgl_awal, '%Y-%m-%d')

    if tgl_akhir == None:
        tgl_akhir = datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_akhir = datetime.strptime(tgl_akhir, '%Y-%m-%d')
    return tgl_awal, tgl_akhir


def get_date_prev(tgl_awal, tgl_akhir):
    tgl_awal = tgl_awal - relativedelta(months=1)
    tgl_awal = tgl_awal.strftime('%Y-%m-%d')
    tgl_akhir = tgl_akhir - relativedelta(months=1)
    tgl_akhir = tgl_akhir.strftime('%Y-%m-%d')
    return tgl_awal, tgl_akhir


def count_values(data, param):
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i][param]] += float(data[i]['total'])
    return cnt


@inventory_bp.route('/inventory/stok_card')
def stok_card():
    return jsonify({"message": "ini data stok card"})


@inventory_bp.route('/inventory/tren_stok')
def tren_stok():
    tgl_awal = request.args.get('tgl_awal')
    tahun = datetime.now().year if tgl_awal == None else int(tgl_awal[:4])
    
    # Get query result
    result = query_tren_stok(tahun)

    tren = {}
    for i in range(1, 13):
        tren[month_id[i]] = {
            "slow_moving": 0,
            "fast_moving": 0,
        }

    for row in result:
        curr_m = month_id[row['Tgl'].month]
        if row['Jml'] > 50:
            tren[curr_m]['fast_moving'] = round(tren[curr_m]['fast_moving'] + float(row['Total']), 2)
        else:
            tren[curr_m]['slow_moving'] = round(tren[curr_m]['slow_moving'] + float(row['Total']), 2)
        
    data = {
        "judul": "Tren Stok Moving",
        "label": 'Inventory',
        "tahun": tahun,
        "tren_moving": tren
    }
    return jsonify(data)


@inventory_bp.route('/inventory/stok_supplier')
def stock_supplier():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    tgl_awal_prev, tgl_akhir_prev = get_date_prev(tgl_awal, tgl_akhir)

    # Get query result
    result = query_stok_supplier(tgl_awal, tgl_akhir + timedelta(days=1))
    result_prev = query_stok_supplier(tgl_awal_prev, datetime.strptime(tgl_akhir_prev, '%Y-%m-%d') + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglTerima'], "supplier": row['NamaSupplier'], "total": row['TotalBayar'], "jumlah": row['jmlorder']} for row in result]
    tmp_prev = [{"tanggal": row['TglTerima'], "supplier": row['NamaSupplier'], "total": row['TotalBayar'], "jumlah": row['jmlorder']} for row in result_prev]

    
    # Extract data as (dataframe)
    cnts = count_values(tmp, 'supplier')
    cnts_prev = count_values(tmp_prev, 'supplier')
    data = [{"name": x, "value": y} for x, y in cnts.items()]
    data_prev = [{"name": x, "value": y} for x, y in cnts_prev.items()]

    # Define trend percentage
    for i in range(len(cnts)):
        percentage = None
        for j in range(len(cnts_prev)):
            if data[i]["name"] == data_prev[j]["name"]:
                percentage = (data[i]["value"] - data_prev[j]["value"]) / data[i]["value"]
            try:
                data[i]["trend"] = round(percentage, 3)
            except:
                data[i]["trend"] = percentage
        data[i]["predict"] = None
    
    # Define return result as a json
    result = {
        "judul": 'Stok per Supplier',
        "label": 'Inventory',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@inventory_bp.route('/inventory/top_produk')
def top_produk():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    
    # Get query result
    result = query_top_produk(tgl_awal, tgl_akhir + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglTransaksi'],
            "item": row['NamaBarang'],
            "stok_awal": row['StokAwal'],
            "stok_akhir": row['StokAkhir'],
            "penggunaan": row['Penggunaan'],} for row in result]

    # Define trend percentage
    stock, stock_in, stock_out = Counter(), Counter(), Counter()
    for i in range(len(tmp)):
        stock[tmp[i]['item']] += tmp[i]['stok_akhir']
        stock_in[tmp[i]['item']] += tmp[i]['stok_awal']
        stock_out[tmp[i]['item']] += tmp[i]['penggunaan']
    data = [{"name": x, "stock": y}#, "in": j, "out": k} 
            for x, y in stock.items()
            # for _, j in stock_in.items()
            # for _, k in stock_out.items()
            ]

    # Define return result as a json
    result = {
        "judul": 'Top Produk',
        "label": 'Inventory',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@inventory_bp.route('/inventory/jenis_produk')
def jenis_produk():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    tgl_awal_prev, tgl_akhir_prev = get_date_prev(tgl_awal, tgl_akhir)

    # Get query result
    result = query_jenis_produk(tgl_awal, tgl_akhir + timedelta(days=1))
    result_prev = query_jenis_produk(tgl_awal_prev, datetime.strptime(tgl_akhir_prev, '%Y-%m-%d') + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglClosing'], "jenis": row['JenisBarang'], "stok": row['StokReal']} for row in result]
    tmp_prev = [{"tanggal": row['TglClosing'], "jenis": row['JenisBarang'], "stok": row['StokReal']} for row in result_prev]


    # Extract data as (dataframe)
    cnts, cnts_prev = Counter(), Counter()
    for i in range(len(tmp)):
        cnts[tmp[i]['jenis']] += tmp[i]['stok']
    for i in range(len(tmp_prev)):
        cnts_prev[tmp_prev[i]['jenis']] += tmp_prev[i]['stok']

    data = [{"name": x, "value": y} for x, y in cnts.items()]
    data_prev = [{"name": x, "value": y} for x, y in cnts_prev.items()]

    # Define trend percentage
    for i in range(len(cnts)):
        percentage = None
        for j in range(len(cnts_prev)):
            if data[i]["name"] == data_prev[j]["name"]:
                percentage = (data[i]["value"] - data_prev[j]["value"]) / data[i]["value"]
            try:
                data[i]["trend"] = round(percentage, 3)
            except:
                data[i]["trend"] = percentage
        data[i]["predict"] = None
    
    # Define return result as a json
    result = {
        "judul": 'Stok per Jenis Barang',
        "label": 'Inventory',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@inventory_bp.route('/inventory/jenis_aset')
def jenis_aset():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    tgl_awal_prev, tgl_akhir_prev = get_date_prev(tgl_awal, tgl_akhir)

    # Get query result
    result = query_jenis_aset(tgl_awal, tgl_akhir + timedelta(days=1))
    result_prev = query_jenis_aset(tgl_awal_prev, datetime.strptime(tgl_akhir_prev, '%Y-%m-%d') + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglClosing'], "jenis": row['JenisBarang'], "stok": row['StokReal']} for row in result]
    tmp_prev = [{"tanggal": row['TglClosing'], "jenis": row['JenisBarang'], "stok": row['StokReal']} for row in result_prev]

    # Extract data as (dataframe)
    cnts, cnts_prev = Counter(), Counter()
    for i in range(len(tmp)):
        cnts[tmp[i]['jenis']] += tmp[i]['stok']
    for i in range(len(tmp_prev)):
        cnts_prev[tmp_prev[i]['jenis']] += tmp_prev[i]['stok']

    data = [{"name": x, "value": y} for x, y in cnts.items()]
    data_prev = [{"name": x, "value": y} for x, y in cnts_prev.items()]

    # Define trend percentage
    for i in range(len(cnts)):
        percentage = None
        for j in range(len(cnts_prev)):
            if data[i]["name"] == data_prev[j]["name"]:
                percentage = (data[i]["value"] - data_prev[j]["value"]) / data[i]["value"]
            try:
                data[i]["trend"] = round(percentage, 3)
            except:
                data[i]["trend"] = percentage
        data[i]["predict"] = None
    
    # Define return result as a json
    result = {
        "judul": 'Stok per Jenis Barang',
        "label": 'Inventory',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@inventory_bp.route('/inventory/detail_stok')
def detail_stok():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)

    # Get query result
    result = query_detail_stok(tgl_awal, tgl_akhir + timedelta(days=1))

    tmp, nama_barang = [], []
    for row in result:
        nama_barang.append(row['NamaBarang'])
        tmp.append({
            "tanggal": row['TglClosing'],
            "jenis": row['JenisBarang'],
            "nama": row['NamaBarang'],
            "asal": row['AsalBarang'],
            "stok": row['StokReal'],
            "harga": row['TotalNetto1'],
            "judul": 'Detail Barang',
            "label": 'Inventory'
        })
    
    cnt = Counter()
    for i in range(len(tmp)):
        cnt[tmp[i]['nama']] = {
            "stok": tmp[i]['stok'],
            "jenis": tmp[i]['jenis'],
            "harga": tmp[i]['harga'],
            "total_harga": tmp[i]['harga'] * tmp[i]['stok'],
            "status": None
        } 

    result = {
        "judul": 'Stok per Jenis Barang',
        "label": 'Inventory',
        "data": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)
