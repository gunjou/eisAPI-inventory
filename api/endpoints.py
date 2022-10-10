from collections import Counter
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from flask import Blueprint, jsonify, request
from sqlalchemy import text

from api.config import MONTH_ID as month_id
from api.config import get_connection

inventory_bp = Blueprint('inventory', __name__)
engine = get_connection()


def get_default_date(tgl_awal, tgl_akhir):
    if tgl_awal == None:
        tgl_awal = datetime.today() - relativedelta(months=1)
        tgl_awal = datetime.strptime(tgl_awal.strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_awal = datetime.strptime(tgl_awal, '%Y-%m-%d')

    if tgl_akhir == None:
        tgl_akhir = datetime.strptime(
            datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_akhir = datetime.strptime(tgl_akhir, '%Y-%m-%d')
    return tgl_awal, tgl_akhir


@inventory_bp.route('/stok_card')
def stok_card():
    return jsonify({"message": "ini data stok card"})


@inventory_bp.route('/tren_stok')
def tren_stok():
    tgl_awal = request.args.get('tgl_awal')
    tahun = datetime.now().year if tgl_awal == None else int(tgl_awal[:4])
    result = engine.execute(
        text(
            f"""SELECT pa.TglPelayanan AS Tgl, pa.JmlBarang AS Jml, 
			pa.HargaSatuan, pa.HargaSatuan*pa.JmlBarang AS Total
			FROM dbo.PemakaianAlkes pa
            WHERE datepart(year,[TglPelayanan]) = {tahun-1}
            OR datepart(year,[TglPelayanan]) = {tahun}
			UNION ALL
			SELECT aj.TglPelayanan AS Tgl, aj.JmlBarang AS Jml, 
			aj.HargaSatuan, aj.HargaSatuan*aj.JmlBarang AS Total
			FROM dbo.ApotikJual aj 
            WHERE datepart(year,[TglPelayanan]) = {tahun-1}
            OR datepart(year,[TglPelayanan]) = {tahun}
           	ORDER BY Tgl ASC;"""))

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


@inventory_bp.route('/stok_supplier')
def stock_supplier():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT st.TglTerima, s.NamaSupplier, dov.jmlorder, ts.TotalBiaya+ts.TotalPpn as TotalBayar
            FROM dbo.DetailOrderVerif dov
            INNER JOIN dbo.TagihanSupplier ts
            ON dov.NoTerima = ts.NoTerima
            INNER JOIN dbo.StrukTerima st
            ON ts.NoTerima = st.NoTerima
            INNER JOIN dbo.Supplier s
            ON st.KdSupplier = s.KdSupplier
            WHERE st.TglTerima >= '{tgl_awal}'
            AND st.TglTerima < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY st.TglTerima ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglTerima'],
            "supplier": row['NamaSupplier'],
            "total": row['TotalBayar'],
            "jumlah": row['jmlorder'],
            "judul": 'Stok per Supplier',
            "label": 'Inventory'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['supplier']] += data[i]['jumlah']

    result = {
        "judul": 'Stok per Supplier',
        "label": 'Inventory',
        "supplier": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@inventory_bp.route('/top_produk')
def top_produk():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT vhpa.TglTransaksi, vhpa.NamaBarang, vhpa.StokAwal, 
			 vhpa.StokAwal-vhpa.StokAkhir AS Penggunaan, vhpa.StokAkhir 
			 FROM dbo.V_H_PemakaianAlkes vhpa
			 WHERE vhpa.TglTransaksi >= '{tgl_awal}'
           	 AND vhpa.TglTransaksi < '{tgl_akhir + timedelta(days=1)}'
           	 ORDER BY vhpa.NamaBarang, vhpa.TglTransaksi ASC;"""))
    
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglTransaksi'],
            "item": row['NamaBarang'],
            "stok_awal": row['StokAwal'],
            "stok_akhir": row['StokAkhir'],
            "penggunaan": row['Penggunaan'],
            "judul": 'Top Produk',
            "label": 'Inventory'
        })
    cnt = Counter()
    for i in range(len(data)):
        try:
            cnt[data[i]['item']] += data[i]['penggunaan']
        except:
            pass

    result = {
        "judul": 'Top Produk',
        "label": 'Inventory',
        "item": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@inventory_bp.route('/jenis_produk')
def jenis_produk():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT vdsbmr.TglClosing, vdsbmr.JenisBarang, vdsbmr.StokReal 
			 FROM dbo.V_DataStokBarangMedisRekap vdsbmr 
			 WHERE vdsbmr.TglClosing >= '{tgl_awal}'
           	 AND vdsbmr.TglClosing < '{tgl_akhir + timedelta(days=1)}'
           	 ORDER BY vdsbmr.TglClosing ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglClosing'],
            "jenis": row['JenisBarang'],
            "stok": row['StokReal'],
            "judul": 'Stok per Jenis Barang',
            "label": 'Inventory'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['jenis']] = data[i]['stok']

    result = {
        "judul": 'Stok per Jenis Barang',
        "label": 'Inventory',
        "supplier": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@inventory_bp.route('/jenis_aset')
def jenis_aset():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT vdsbnmr.TglClosing, vdsbnmr.JenisBarang, vdsbnmr.StokReal 
			 FROM dbo.V_DataStokBarangNonMedisRekapx vdsbnmr  
			 WHERE vdsbnmr.TglClosing >= '{tgl_awal}'
           	 AND vdsbnmr.TglClosing < '{tgl_akhir + timedelta(days=1)}'
           	 ORDER BY vdsbnmr.TglClosing ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglClosing'],
            "jenis": row['JenisBarang'],
            "stok": row['StokReal'],
            "judul": 'Stok per Jenis Barang',
            "label": 'Inventory'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['jenis']] = data[i]['stok']

    result = {
        "judul": 'Stok per Jenis Barang',
        "label": 'Inventory',
        "supplier": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@inventory_bp.route('/detail_stok')
def detail_stok():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT vdsbmr.TglClosing, vdsbmr.JenisBarang, vdsbmr.NamaBarang, 
            vdsbmr.AsalBarang, vdsbmr.StokReal, vdsbmr.TotalNetto1
			 FROM dbo.V_DataStokBarangMedisRekap vdsbmr 
			 WHERE vdsbmr.TglClosing >= '{tgl_awal}'
           	 AND vdsbmr.TglClosing < '{tgl_akhir + timedelta(days=1)}'
           	 ORDER BY vdsbmr.TglClosing ASC;"""))
    data, nama_barang = [], []
    for row in result:
        nama_barang.append(row['NamaBarang'])
        data.append({
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
    for i in range(len(data)):
        cnt[data[i]['nama']] = {
            "stok": data[i]['stok'],
            "jenis": data[i]['jenis'],
            "harga": data[i]['harga'],
            "total_harga": data[i]['harga'] * data[i]['stok'],
            "status": None
        } 

    result = {
        "judul": 'Stok per Jenis Barang',
        "label": 'Inventory',
        "detail": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)
