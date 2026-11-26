#!/usr/bin/env python3
"""
Script diário: cruza Silver com AWS Price List (boto3 pricing) com cache e fallback,
e calcula custo estimado de downtime por instância respeitando vigência histórica do
valor médio por 'Established' (valor por conexão estabelecida).

Saída: ./track-commerce/gold_costs/<empresa>/<identifier>_downtime_<YYYY-MM-DD>.json
 (opcional upload para AWS_BUCKET_GOLD se configurado).

Parar ao final (não executa automaticamente).
"""
from __future__ import annotations
import os, json, math, csv, traceback
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from statistics import median
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
DIR_BASE = Path(os.getenv("TRACKCOMMERCE_DIR")) if os.getenv("TRACKCOMMERCE_DIR") else (SCRIPT_DIR / "track-commerce")
DIR_SILVER = DIR_BASE / "silver"
DIR_GOLD_COSTS = DIR_BASE / "gold_costs"
CACHE_FILE = SCRIPT_DIR / ".pricing_cache.json"
RATES_FILE = SCRIPT_DIR / "instance_rates.json"  # usuário pode gerenciar

# Config / constantes
HOURS_PER_MONTH = 730.0
DEFAULT_VALUE_PER_ESTABLISHED = float(os.getenv("DEFAULT_VALUE_PER_ESTABLISHED", "0.001"))  # USD por connection-minute
DEFAULT_CRITICAL_FACTOR = float(os.getenv("DEFAULT_CRITICAL_FACTOR", "1.0"))
FALLBACK_PER_VCPU_HOUR = float(os.getenv("FALLBACK_PER_VCPU_HOUR", "0.02"))
FALLBACK_PER_GB_RAM_HOUR = float(os.getenv("FALLBACK_PER_GB_RAM_HOUR", "0.005"))
FALLBACK_PER_GB_DISK_MONTH = float(os.getenv("FALLBACK_PER_GB_DISK_MONTH", "0.01"))

AWS_BUCKET_GOLD = os.getenv("AWS_BUCKET_GOLD", "").strip()

# mapeamento region_code -> location name usado pela AWS Pricing API
REGION_TO_LOCATION = {
    "us-east-1": "US East (N. Virginia)",
    "us-west-2": "US West (Oregon)",
    "eu-west-1": "EU (Ireland)",
    "sa-east-1": "South America (Sao Paulo)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
}

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:
    boto3 = None

try:
    import mysql.connector as mysql_connector
except Exception:
    mysql_connector = None


def load_cache():
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_cache(cache):
    try:
        CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def pricing_lookup(instance_type: str, region_code: str, cache: dict) -> dict:
    """Tenta obter preço horário via AWS Price List API; usa cache; retorna dict com keys:
    hourly (float) ou None e basis ('aws_pricing_api'|'parametric_estimate')"""
    key = f"{instance_type}||{region_code}"
    if key in cache:
        return {"hourly": cache[key].get("hourly"), "basis": cache[key].get("basis", "cache")}

    # tentar AWS Pricing
    price_hourly = None
    basis = "parametric_estimate"
    if boto3:
        try:
            client = boto3.client("pricing", region_name="us-east-1")
            # tentar filtros simples; location necessita nome amigável -> tentar algumas alternativas
            location_candidates = [region_code, region_code.replace("-", " ").title(), REGION_TO_LOCATION.get(region_code, "")]
            for loc in filter(None, location_candidates):
                filters = [
                    {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                    {"Type": "TERM_MATCH", "Field": "location", "Value": loc},
                    {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                ]
                resp = client.get_products(ServiceCode="AmazonEC2", Filters=filters, MaxResults=50)
                for prod in resp.get("PriceList", []):
                    try:
                        j = json.loads(prod)
                        # navegar por terms -> OnDemand -> priceDimensions
                        terms = j.get("terms", {}).get("OnDemand", {})
                        for term in terms.values():
                            for dim in term.get("priceDimensions", {}).values():
                                price_str = dim.get("pricePerUnit", {}).get("USD")
                                if price_str:
                                    price_hourly = float(price_str)
                                    basis = "aws_pricing_api"
                                    raise StopIteration
                    except StopIteration:
                        break
                if price_hourly is not None:
                    break
        except Exception:
            price_hourly = None

    # fallback parametric
    if price_hourly is None:
        price_hourly = parametric_estimate(instance_type)
        basis = "parametric_estimate"

    cache[key] = {"hourly": price_hourly, "basis": basis, "ts": datetime.now(timezone.utc).isoformat()}
    save_cache(cache)
    return {"hourly": price_hourly, "basis": basis}


def parametric_estimate(instance_type: str = None, vcpus: int | None = None, ram_gb: float | None = None, disk_gb: float | None = None) -> float:
    """Estimativa simples por tabela paramétrica usando vcpus/ram/disk quando disponível."""
    if vcpus is None and ram_gb is None and instance_type:
        # extrair vcpu/ram de nomes comuns (ex.: t3.small) não implementado; usar fallback médio
        vcpus = 1
        ram_gb = 1.0
    v = vcpus or 0
    r = ram_gb or 0.0
    d = disk_gb or 0.0
    hourly = v * FALLBACK_PER_VCPU_HOUR + r * FALLBACK_PER_GB_RAM_HOUR
    # disk monthly -> convert to hourly share
    hourly += (d * FALLBACK_PER_GB_DISK_MONTH) / HOURS_PER_MONTH
    return round(hourly, 6)


def _normalizar_identificador(valor: str | None) -> str:
    return (valor or "").replace("-", "").replace("_", "").strip().lower()


def _conectar_bd():
    """Abre conexão com o MySQL quando configurado e disponível; caso contrário retorna None."""
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    # aceitar nomes alternativos para compatibilidade com o frontend/backend
    db_pass = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME") or os.getenv("DB_DATABASE")
    if not (db_host and db_user and db_pass and db_name):
        return None
    if mysql_connector is None:
        return None
    try:
        return mysql_connector.connect(host=db_host, user=db_user, password=db_pass, database=db_name)
    except Exception:
        return None


def carregar_instancias_bd() -> dict | None:
    """Carrega do BD o status (ativo) e valor_por_established de todas as instâncias cadastradas."""
    conn = _conectar_bd()
    if conn is None:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id_instancia, identificador, valor_por_established, ativo FROM instancia")
        mapa = {"por_exato": {}, "por_normalizado": {}}
        for row in cursor.fetchall():
            inst_id, ident, valor_est, ativo = row[0], row[1], row[2], row[3]
            if not ident:
                continue
            rate = None
            if valor_est is not None:
                try:
                    rate = float(valor_est)
                except Exception:
                    rate = None
            registro = {
                "id_instancia": inst_id,
                "identificador": ident,
                "valor_por_established": rate,
                "ativo": bool(ativo),
            }
            mapa["por_exato"][ident] = registro
            norm = _normalizar_identificador(ident)
            if norm:
                mapa["por_normalizado"][norm] = registro
        return mapa
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _buscar_instancia_no_mapa(mapa: dict, identifier: str) -> dict | None:
    if identifier in mapa["por_exato"]:
        return mapa["por_exato"][identifier]
    return mapa["por_normalizado"].get(_normalizar_identificador(identifier))


def get_rate_from_db(identifier: str, target_date: date):
    """Consulta o banco para obter o valor vigente por 'Established' de instância ativa."""
    conn = _conectar_bd()
    if conn is None:
        return None
    try:
        cursor = conn.cursor()
        # tentar match exato pelo identificador apenas para instâncias ativas
        cursor.execute(
            "SELECT valor_por_established FROM instancia WHERE identificador = %s AND ativo = 1 LIMIT 1",
            (identifier,),
        )
        row = cursor.fetchone()
        if not row:
            # tentar normalizado (remover - e _) entre instâncias ativas
            norm = _normalizar_identificador(identifier)
            cursor.execute("SELECT identificador, valor_por_established FROM instancia WHERE ativo = 1")
            for ident, valor_est in cursor.fetchall():
                if ident and _normalizar_identificador(ident) == norm:
                    row = (valor_est,)
                    break
        if row and row[0] is not None:
            try:
                return float(row[0])
            except Exception:
                return None
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass
 


def analyze_silver_for_instance(silver_path: Path, report_date: date):
    """Lê CSV Silver e calcula avg_established, vcpus, ram_gb, disk_gb e downtime minutes in last day."""
    if not silver_path.exists():
        return None
    timestamps = []
    established = []
    vcpus = None; ram_gb = None; disk_gb = None; declared_monthly_cost = None
    with silver_path.open(encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f, delimiter=";")
        last_row = None
        for r in rdr:
            ts = r.get("timestamp")
            try:
                t = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            if t.date() != report_date:
                continue
            timestamps.append(t)
            last_row = r
            try:
                established.append(float(r.get("rede_established") or 0))
            except Exception:
                pass
            if vcpus is None:
                try:
                    vcpus = int(r.get("vcpus") or r.get("cpu_nucleos_logicos") or r.get("cpu_count_logical"))
                except Exception:
                    vcpus = vcpus
            if ram_gb is None:
                try:
                    ram_gb = float(r.get("ram_total_gb") or r.get("ram_gb") or 0.0)
                except Exception:
                    pass
            if disk_gb is None:
                try:
                    disk_gb = float(r.get("disco_total_gb") or r.get("disk_gb") or 0.0)
                except Exception:
                    pass
            if declared_monthly_cost is None:
                try:
                    declared_monthly_cost = float(r.get("declared_monthly_cost"))
                except Exception:
                    pass
    if not timestamps:
        return None
    timestamps.sort()
    # compute expected_interval as median of deltas under 1 hour
    deltas = [ (timestamps[i+1]-timestamps[i]).total_seconds() for i in range(len(timestamps)-1) ]
    small = [d for d in deltas if d < 3600 and d>0]
    expected = median(small) if small else 300.0
    # downtime: gaps > expected -> excess is downtime
    downtime_seconds = sum(max(0, d - expected) for d in deltas if d > expected)
    downtime_minutes = round(downtime_seconds/60.0, 2)
    avg_established = round(sum(established)/len(established), 3) if established else 0.0
    result = {
        "vcpus": vcpus, "ram_gb": ram_gb, "disk_gb": disk_gb,
        "declared_monthly_cost": declared_monthly_cost,
        "avg_established": avg_established,
        "downtime_minutes": downtime_minutes,
        "expected_interval_seconds": expected,
        "samples": len(timestamps),
    }
    # incluir metadados do último registro (se houver) para evitar re-leitura do CSV
    if last_row:
        result["ec2_instance_type"] = last_row.get("ec2_instance_type") or last_row.get("instance_type")
        result["ec2_region"] = last_row.get("ec2_region") or last_row.get("region")
        try:
            dec = last_row.get("declared_monthly_cost")
            if dec:
                result["declared_monthly_cost"] = float(dec)
        except Exception:
            pass
    return result


def compute_costs_for_instance(info: dict, pricing_res: dict, rate_used: float, critical_factor: float):
    hourly = pricing_res.get("hourly")
    basis = pricing_res.get("basis")
    monthly_cost = None
    if hourly is not None:
        monthly_cost = round(hourly * HOURS_PER_MONTH, 6)
    elif info.get("declared_monthly_cost"):
        monthly_cost = info["declared_monthly_cost"]
        hourly = monthly_cost / HOURS_PER_MONTH
    else:
        # parametric fallback monthly
        hourly = parametric_estimate(vcpus=info.get("vcpus"), ram_gb=info.get("ram_gb"), disk_gb=info.get("disk_gb"))
        monthly_cost = round(hourly * HOURS_PER_MONTH, 6)
    cost_per_min = hourly / 60.0 if hourly else None

    infra_cost_during_downtime = round((cost_per_min or 0.0) * info["downtime_minutes"], 6)

    revenue_loss = round(rate_used * info["avg_established"] * info["downtime_minutes"], 6) if rate_used and info["avg_established"] else 0.0

    # composed model: prioritize revenue_loss when configured
    if rate_used and rate_used > 0:
        total = round(revenue_loss + infra_cost_during_downtime * 0.1 * critical_factor, 6)
        calculation_basis = "business_impact_with_infra_fraction"
    else:
        total = round(infra_cost_during_downtime * critical_factor, 6)
        calculation_basis = "infra_cost_proportional"

    return {
        "monthly_cost": monthly_cost,
        "hourly_cost": hourly,
        "cost_per_minute": round(cost_per_min, 9) if cost_per_min else None,
        "infra_cost_during_downtime": infra_cost_during_downtime,
        "estimated_revenue_loss": revenue_loss,
        "total_estimated_downtime_cost": total,
        "calculation_basis": calculation_basis,
        "pricing_basis": basis,
    }


# upload_to_s3_local removed: direct S3 put_object is used instead; fallback writes local files when needed.


def s3_get_json(bucket: str, key: str):
    if not boto3 or not bucket:
        return None
    try:
        s3 = boto3.client("s3")
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception:
        return None


def s3_put_json(bucket: str, key: str, obj: dict):
    if not boto3 or not bucket:
        return False
    try:
        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"), ContentType="application/json")
        return True
    except Exception:
        return False


def main(report_date: date | None = None):
    report_date = report_date or date.today() - timedelta(days=1)  # geralmente rodar para dia anterior
    cache = load_cache()
    DIR_GOLD_COSTS.mkdir(parents=True, exist_ok=True)
    results = []

    # percorrer silver por empresa
    if not DIR_SILVER.exists():
        print("Pasta silver não encontrada. Abortando.")
        return
    instancias_bd = carregar_instancias_bd()
    for pasta in sorted(DIR_SILVER.iterdir()):
        if not pasta.is_dir():
            continue
        empresa = pasta.name
        out_dir = DIR_GOLD_COSTS / empresa
        out_dir.mkdir(parents=True, exist_ok=True)
        for csvfile in sorted(pasta.glob("*_sistema.csv")):
            identifier = csvfile.stem.replace("_sistema","")
            try:
                if instancias_bd is not None:
                    inst_bd = _buscar_instancia_no_mapa(instancias_bd, identifier)
                    if not inst_bd or not inst_bd["ativo"]:
                        print(f"[INFO] Instância '{identifier}' inativa ou não cadastrada no BD; atualização de preço ignorada.")
                        continue
                    rate_used = inst_bd["valor_por_established"]
                else:
                    rate_used = None

                info = analyze_silver_for_instance(csvfile, report_date)
                if not info:
                    continue
                # obtain instance type and region from analyze result (avoids re-reading CSV)
                inst_type = info.get("ec2_instance_type")
                region_code = info.get("ec2_region")

                pricing_res = pricing_lookup(inst_type or "", region_code or "", cache)
                # se o BD estiver indisponível (rate_used is None), não aplicar fallback a fontes alternativas
                critical_factor = DEFAULT_CRITICAL_FACTOR
                costs = compute_costs_for_instance(info, pricing_res, rate_used, critical_factor)

                out = {
                    "date": report_date.isoformat(),
                    "empresa": empresa,
                    "identifier": identifier,
                    "info": info,
                    "pricing": pricing_res,
                    "rate_used": rate_used,
                    "critical_factor": critical_factor,
                    "costs": costs,
                }
                # prepare payload json bytes and upload directly to S3 (no local file)
                key = f"{empresa}/{identifier}_downtime_{report_date.isoformat()}.json"
                body = json.dumps(out, ensure_ascii=False, indent=2).encode("utf-8")
                uploaded = False
                if boto3 and AWS_BUCKET_GOLD:
                    try:
                        s3 = boto3.client("s3")
                        s3.put_object(Bucket=AWS_BUCKET_GOLD, Key=key, Body=body, ContentType="application/json")
                        uploaded = True
                    except Exception:
                        uploaded = False
                if not uploaded:
                    # fallback: persist locally if S3 unavailable (keeps previous behaviour safe)
                    out_path = out_dir / f"{identifier}_downtime_{report_date.isoformat()}.json"
                    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                    key = f"{empresa}/{out_path.name}"

                # --- atualizar histórico consolidado por empresa em GOLD (in-memory then S3) ---
                try:
                    hist_key = f"{empresa}/pricing_history.json"
                    hist = s3_get_json(AWS_BUCKET_GOLD, hist_key) if boto3 and AWS_BUCKET_GOLD else None
                    if hist is None:
                        # try local file as fallback
                        gold_dir = DIR_BASE / "gold" / empresa
                        hist_path = gold_dir / "pricing_history.json"
                        if hist_path.exists():
                            hist = json.loads(hist_path.read_text(encoding="utf-8"))
                        else:
                            hist = {"empresa": empresa, "instancias": []}

                    # localizar ou criar entrada para a instancia
                    inst_entry = None
                    for e in hist["instancias"]:
                        inst_name = (e.get("instancia", "") or "").replace("-", "").replace("_", "").lower()
                        inst_id_norm = identifier.replace("-", "").replace("_", "").lower()
                        if inst_name == inst_id_norm:
                            inst_entry = e
                            break
                    if inst_entry is None:
                        inst_entry = {"instancia": identifier, "historico_precos": []}
                        hist["instancias"].append(inst_entry)

                    hist_record = {
                        "date": report_date.isoformat(),
                        "info": info,
                        "pricing": pricing_res,
                        "costs": costs,
                        "rate_used": rate_used,
                        "calculation_basis": costs.get("calculation_basis"),
                    }
                    inst_entry["historico_precos"].append(hist_record)

                    # upload consolidated history to S3 if possible, else write local
                    if boto3 and AWS_BUCKET_GOLD:
                        s3_put_json(AWS_BUCKET_GOLD, hist_key, hist)
                    else:
                        gold_dir = DIR_BASE / "gold" / empresa
                        gold_dir.mkdir(parents=True, exist_ok=True)
                        hist_path = gold_dir / "pricing_history.json"
                        hist_path.write_text(json.dumps(hist, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass
                results.append(key)
            except Exception:
                print(f"Falha ao processar {csvfile}:")
                traceback.print_exc()

    print(f"Concluído: {len(results)} relatórios gerados em {DIR_GOLD_COSTS}")


# (REGION_TO_LOCATION defined in constants above)

if __name__ == "__main__":
    main()
