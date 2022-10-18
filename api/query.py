from sqlalchemy import text

from api.config import get_connection

engine = get_connection()


def query_tren_stok(tahun):
    result = engine.execute(
        text(f"""SELECT pa.TglPelayanan AS Tgl, pa.JmlBarang AS Jml, 
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
    return result


def query_stok_supplier(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT st.TglTerima, s.NamaSupplier, dov.jmlorder, ts.TotalBiaya+ts.TotalPpn as TotalBayar
            FROM dbo.DetailOrderVerif dov
            INNER JOIN dbo.TagihanSupplier ts
            ON dov.NoTerima = ts.NoTerima
            INNER JOIN dbo.StrukTerima st
            ON ts.NoTerima = st.NoTerima
            INNER JOIN dbo.Supplier s
            ON st.KdSupplier = s.KdSupplier
            WHERE st.TglTerima >= '{start_date}'
            AND st.TglTerima < '{end_date}'
            ORDER BY st.TglTerima ASC;"""))
    return result


def query_top_produk(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT vhpa.TglTransaksi, vhpa.NamaBarang, vhpa.StokAwal, 
			vhpa.StokAwal-vhpa.StokAkhir AS Penggunaan, vhpa.StokAkhir 
			FROM dbo.V_H_PemakaianAlkes vhpa
			WHERE vhpa.TglTransaksi >= '{start_date}'
           	AND vhpa.TglTransaksi < '{end_date}'
           	ORDER BY vhpa.NamaBarang, vhpa.TglTransaksi ASC;"""))
    return result


def query_jenis_produk(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT vdsbmr.TglClosing, vdsbmr.JenisBarang, vdsbmr.StokReal 
			FROM dbo.V_DataStokBarangMedisRekap vdsbmr 
			WHERE vdsbmr.TglClosing >= '{start_date}'
           	AND vdsbmr.TglClosing < '{end_date}'
           	ORDER BY vdsbmr.TglClosing ASC;"""))
    return result


def query_jenis_aset(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT vdsbnmr.TglClosing, vdsbnmr.JenisBarang, vdsbnmr.StokReal 
			FROM dbo.V_DataStokBarangNonMedisRekapx vdsbnmr  
			WHERE vdsbnmr.TglClosing >= '{start_date}'
           	AND vdsbnmr.TglClosing < '{end_date}'
           	ORDER BY vdsbnmr.TglClosing ASC;"""))
    return result


def query_detail_stok(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT vdsbmr.TglClosing, vdsbmr.JenisBarang, vdsbmr.NamaBarang, 
            vdsbmr.AsalBarang, vdsbmr.StokReal, vdsbmr.TotalNetto1
			FROM dbo.V_DataStokBarangMedisRekap vdsbmr 
			WHERE vdsbmr.TglClosing >= '{start_date}'
           	AND vdsbmr.TglClosing < '{end_date}'
           	ORDER BY vdsbmr.TglClosing ASC;"""))
    return result