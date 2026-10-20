import hmac
import json
import os
import re
import time

import boto3

ATHENA_REGION = os.environ.get("ATHENA_REGION", "us-east-1")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "cootravale-s3-athena")
OUTPUT_LOCATION = os.environ.get("ATHENA_OUTPUT", "s3://cootravale-athena-rcm/")
MAX_WAIT_SECONDS = int(os.environ.get("ATHENA_MAX_WAIT", "25"))

athena = boto3.client("athena", region_name=ATHENA_REGION)

PLACA_RE = re.compile(r"^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$")  # ABC1234 e Mercosul ABC1D23

# placa -> veiculo -> termos de agregacao do veiculo, com o termo (cooperado)
QUERY = """
SELECT v.placa,
       CAST(v.veiculo_id AS int) AS veiculo_id,
       CAST(t.termo_id AS int) AS termo_id,
       CAST(t.cod_pessoa AS int) AS cod_pessoa,
       t.data_agregacao,
       t.data_desagregacao,
       tv.data_inicio,
       tv.data_termino,
       p.identificador AS cpf_cnpj,
       c.cooperado,
       c.tpt_descricao
FROM prd_bronze.kmm_veiculo_veiculo v
LEFT JOIN prd_bronze.kmm_agreg_termo_veiculo tv ON tv.veiculo_id = v.veiculo_id
LEFT JOIN prd_bronze.kmm_agreg_termo t ON t.termo_id = tv.termo_id
LEFT JOIN prd_bronze.kmm_kss_pessoa p ON t.cod_pessoa = p.cod_pessoa
LEFT JOIN prd_gold.dim_cooperado c
       ON c.fl_corrente = 1
      AND TRY_CAST(REGEXP_REPLACE(p.identificador, '[^0-9]', '') AS BIGINT)
        = TRY_CAST(REGEXP_REPLACE(c.nk_cooperado, '[^0-9]', '') AS BIGINT)
WHERE v.placa = ?
"""


def _resp(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, ensure_ascii=False, default=str),
    }


def _get_placa(event):
    qs = event.get("queryStringParameters") or {}
    placa = qs.get("placa")
    if not placa and event.get("body"):
        try:
            placa = json.loads(event["body"]).get("placa")
        except (ValueError, AttributeError):
            pass
    return (placa or "").strip().upper().replace("-", "")


def _run_query(placa):
    qid = athena.start_query_execution(
        QueryString=QUERY,
        ExecutionParameters=[f"'{placa}'"],  # placa ja validada pelo regex
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )["QueryExecutionId"]

    deadline = time.time() + MAX_WAIT_SECONDS
    while time.time() < deadline:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(status.get("StateChangeReason", state))
        time.sleep(0.5)
    else:
        athena.stop_query_execution(QueryExecutionId=qid)
        raise TimeoutError("Athena excedeu o tempo limite")

    rows = []
    cols = None
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            vals = [c.get("VarCharValue") for c in r["Data"]]
            if cols is None:
                cols = vals  # primeira linha e o cabecalho
            else:
                rows.append(dict(zip(cols, vals)))
    return rows


def _autorizado(event):
    esperado = os.environ.get("API_KEY", "")
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    enviado = headers.get("x-api-key", "")
    return bool(esperado) and hmac.compare_digest(enviado, esperado)


def lambda_handler(event, context):
    if not _autorizado(event):
        return _resp(401, {"erro": "Nao autorizado"})
    placa = _get_placa(event)
    if not PLACA_RE.match(placa):
        return _resp(400, {"erro": "Informe ?placa=ABC1D23 (formato valido)"})
    try:
        rows = _run_query(placa)
    except TimeoutError as e:
        return _resp(504, {"erro": str(e)})
    except Exception as e:
        print(f"erro athena: {e}")
        return _resp(500, {"erro": "Falha ao consultar o Athena"})
    # formato lido pelo campo dinamico Webservice do OTRS (backend ResponseValues)
    if not rows:
        return _resp(404, {"response": {"returncode": "404", "returnmessage": "Placa nao encontrada"},
                           "placa": placa, "values": []})
    return _resp(200, {"response": {"returncode": "200", "returnmessage": "Success"},
                       "placa": placa, "values": rows})
