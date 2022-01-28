import os
import csv
import datetime
import dateutil
import logging
import pyarrow
import pyarrow.parquet as pq
import s3fs

logging.basicConfig(level=logging.INFO, format='[%(asctime)s %(levelname)s %(name)s] %(message)s')
logger = logging.getLogger(__name__)

def parquet_file_writer(data_lake_credentials, parquet_filename, data):
    try:
        schema = pyarrow.schema(
            [
                ('referencia_externa', pyarrow.string()),
                ('entidade_registradora', pyarrow.string()),
                ('instituicao_credenciadora_subcredenciadora', pyarrow.string()),
                ('usuario_final_recebedor', pyarrow.string()),
                ('arranjo_pagamento', pyarrow.string()),
                ('data_liquidacao', pyarrow.date32()),
                ('titular_unidade_recebivel', pyarrow.string()),
                ('constituicao_unidade_recebivel', pyarrow.string()),
                ('valor_constituido_total', pyarrow.float64()),
                ('valor_constituido_antecipacao_pre_contratada', pyarrow.float64()),
                ('valor_bloqueado', pyarrow.float64()),
                ('informacoes_pagamento_unidade_recebivel', pyarrow.list_(
                    pyarrow.struct(
                        [
                            ('numero_documento_titular_domicilio', pyarrow.string()),
                            ('tipo_conta', pyarrow.string()),
                            ('compe', pyarrow.string()),
                            ('ispb', pyarrow.string()),
                            ('agencia', pyarrow.string()),
                            ('numero_conta_numero_conta_pagamento', pyarrow.string()),
                            ('valor_pagar', pyarrow.float64()),
                            ('beneficiario', pyarrow.string()),
                            ('data_liquidacao_efetiva', pyarrow.date32()),
                            ('valor_liquidacao_efetiva', pyarrow.float64()),
                            ('regra_divisao', pyarrow.string()),
                            ('valor_onerado_unidade_recebivel', pyarrow.float64()),
                            ('tipo_informacao_pagamento', pyarrow.string()),
                            ('indicador_ordem_efeito', pyarrow.string()),
                            ('valor_constituido_efeito_contrato_unidade_recebivel', pyarrow.float64())
                        ]
                    )
                )),
                ('carteira', pyarrow.string()),
                ('valor_livre', pyarrow.float64()),
                ('valor_total_ur', pyarrow.float64()),
                ('data_hora_ultima_atualizacao_ur', pyarrow.timestamp('ms')),
                ('identificacao_registradora', pyarrow.int16()),
                ('identificacao_participante', pyarrow.int32()),
                ('nome_arquivo', pyarrow.string()),
                ('identificacao_processo_carga_arquivo', pyarrow.int16()),
                ('data_envio_arquivo', pyarrow.date32()),
                ('data_modificacao_arquivo', pyarrow.timestamp('ms'))
            ]
        )

        s3_data_lake = s3fs.S3FileSystem(
            anon=False,
            key=data_lake_credentials['access_key_id'],
            secret=data_lake_credentials['secret_access_key']
        )

        table = pyarrow.Table.from_pydict(data, schema)
        writer = pq.ParquetWriter(
            where=parquet_filename,
            schema=table.schema,
            data_page_size=128 * 1024 * 1024,
            compression='GZIP',
            filesystem=s3_data_lake
        )
        writer.write_table(table)
        writer.close()
    except Exception as e:
        logger.critical(e)

        
def csv_file_reader(data_source_credentials, csv_filename, data_lake_credentials, data_lake_bucket):
    
    def set_str(value):
        if value:
            return str(value)
        else:
            return None
        
    def set_int(value):
        if value:
            return int(value)
        else:
            return None
        
    def set_float(value):
        if value:
            return float(value)
        else:
            return None
        
    def set_date_iso_8601(value):
        if value:
            return int((datetime.date.fromisoformat(value) - datetime.date(1970,1,1)).days)
        else:
            return None
        
    def set_date_time_iso_8601(value):
        if value:
            return int(datetime.datetime.timestamp(dateutil.parser.parse(value)) * 1000)
        else:
            return None
        
    def set_identificacao_participante(value):
        if value:
            return int(value.split('_')[1])
        else:
            return None
        
    def set_data_envio_arquivo(value):
        if value:
            return set_date_iso_8601(datetime.datetime.strptime(value.split('_')[2], '%Y%m%d').strftime('%Y-%m-%d'))
        else:
            return None
        
    def set_data_modificacao_arquivo(value):
        if value:
            return int(value['LastModified'].timestamp() * 1000)
        else:
            None
    
    column_names = [
        'referencia_externa',
        'entidade_registradora',
        'instituicao_credenciadora_subcredenciadora',
        'usuario_final_recebedor',
        'arranjo_pagamento',
        'data_liquidacao',
        'titular_unidade_recebivel',
        'constituicao_unidade_recebivel',
        'valor_constituido_total',
        'valor_constituido_antecipacao_pre_contratada',
        'valor_bloqueado',
        'informacoes_pagamento_unidade_recebivel',
        'carteira',
        'valor_livre',
        'valor_total_ur',
        'data_hora_ultima_atualizacao_ur',
        'identificacao_registradora',
        'identificacao_participante',
        'nome_arquivo',
        'identificacao_processo_carga_arquivo',
        'data_envio_arquivo',
        'data_modificacao_arquivo'
        
    ]

    subcolumn_names = [
        'numero_documento_titular_domicilio',
        'tipo_conta',
        'compe',
        'ispb',
        'agencia',
        'numero_conta_numero_conta_pagamento',
        'valor_pagar',
        'beneficiario',
        'data_liquidacao_efetiva',
        'valor_liquidacao_efetiva',
        'regra_divisao',
        'valor_onerado_unidade_recebivel',
        'tipo_informacao_pagamento',
        'indicador_ordem_efeito',
        'valor_constituido_efeito_contrato_unidade_recebivel'
    ]

    referencia_externa = []
    entidade_registradora = []
    instituicao_credenciadora_subcredenciadora = []
    usuario_final_recebedor = []
    arranjo_pagamento = []
    data_liquidacao = []
    titular_unidade_recebivel = []
    constituicao_unidade_recebivel = []
    valor_constituido_total = []
    valor_constituido_antecipacao_pre_contratada = []
    valor_bloqueado = []
    informacoes_pagamento_unidade_recebivel = []
    carteira = []
    valor_livre = []
    valor_total_ur = []
    data_hora_ultima_atualizacao_ur = []
    identificacao_registradora = []
    identificacao_participante = []
    nome_arquivo = []
    identificacao_processo_carga_arquivo = []
    data_envio_arquivo = []
    data_modificacao_arquivo = []
    

    chunksize = 1000000
    c = 0
    l = 0
    p = 1
    
    try:
        s3_data_source = s3fs.S3FileSystem(
            anon=False,
            key=data_source_credentials['access_key_id'],
            secret=data_source_credentials['secret_access_key']
        )
        
        _identificacao_registradora = set_int(1)
        _identificacao_participante = set_identificacao_participante(csv_filename)
        _nome_arquivo = set_str(csv_filename)
        _identificacao_processo_carga_arquivo = set_int(1)
        _data_envio_arquivo = set_data_envio_arquivo(csv_filename)
        _data_modificacao_arquivo = set_data_modificacao_arquivo(s3_data_source.info(csv_filename))

        file = s3_data_source.open(csv_filename, 'rb')
        while True:
            row = file.readline()
            if row:
                for object in csv.DictReader(f=row.decode().split('\n'), fieldnames=column_names, delimiter=';', quotechar='"'):
                    referencia_externa.append(set_str(object['referencia_externa']))
                    entidade_registradora.append(set_str(object['entidade_registradora']))
                    instituicao_credenciadora_subcredenciadora.append(set_str(object['instituicao_credenciadora_subcredenciadora']))
                    usuario_final_recebedor.append(set_str(object['usuario_final_recebedor']))
                    arranjo_pagamento.append(set_str(object['arranjo_pagamento']))
                    data_liquidacao.append(set_date_iso_8601(object['data_liquidacao']))
                    titular_unidade_recebivel.append(set_str(object['titular_unidade_recebivel']))
                    constituicao_unidade_recebivel.append(set_str(object['constituicao_unidade_recebivel']))
                    valor_constituido_total.append(set_float(object['valor_constituido_total']))
                    valor_constituido_antecipacao_pre_contratada.append(set_float(object['valor_constituido_antecipacao_pre_contratada']))
                    valor_bloqueado.append(set_float(object['valor_bloqueado']))
                    nested_object = []
                    for nested_row in csv.DictReader(f=object['informacoes_pagamento_unidade_recebivel'].split('|'), fieldnames=subcolumn_names, delimiter=';', quotechar='"'):
                        nested_row['numero_documento_titular_domicilio'] = set_str(nested_row['numero_documento_titular_domicilio'])
                        nested_row['tipo_conta'] = set_str(nested_row['tipo_conta'])
                        nested_row['compe'] = set_str(nested_row['compe'])
                        nested_row['ispb'] = set_str(nested_row['ispb'])
                        nested_row['agencia'] = set_str(nested_row['agencia'])
                        nested_row['numero_conta_numero_conta_pagamento'] = set_str(nested_row['numero_conta_numero_conta_pagamento'])
                        nested_row['valor_pagar'] = set_float(nested_row['valor_pagar'])
                        nested_row['beneficiario'] = set_str(nested_row['beneficiario'])
                        nested_row['data_liquidacao_efetiva'] = set_date_iso_8601(nested_row['data_liquidacao_efetiva'])
                        nested_row['valor_liquidacao_efetiva'] = set_float(nested_row['valor_liquidacao_efetiva'])
                        nested_row['regra_divisao'] = set_str(nested_row['regra_divisao'])
                        nested_row['valor_onerado_unidade_recebivel'] = set_float(nested_row['valor_onerado_unidade_recebivel'])
                        nested_row['tipo_informacao_pagamento'] = set_str(nested_row['tipo_informacao_pagamento'])
                        nested_row['indicador_ordem_efeito'] = set_str(nested_row['indicador_ordem_efeito'])
                        nested_row['valor_constituido_efeito_contrato_unidade_recebivel'] = set_float(nested_row['valor_constituido_efeito_contrato_unidade_recebivel'])
                        nested_object.append(nested_row)
                    informacoes_pagamento_unidade_recebivel.append(nested_object)
                    carteira.append(set_str(object['carteira']))
                    valor_livre.append(set_float(object['valor_livre']))
                    valor_total_ur.append(set_float(object['valor_total_ur']))
                    data_hora_ultima_atualizacao_ur.append(set_date_time_iso_8601(object['data_hora_ultima_atualizacao_ur']))
                    identificacao_registradora.append(_identificacao_registradora)
                    identificacao_participante.append(_identificacao_participante)
                    nome_arquivo.append(_nome_arquivo)
                    identificacao_processo_carga_arquivo.append(_identificacao_processo_carga_arquivo)
                    data_envio_arquivo.append(_data_envio_arquivo)
                    data_modificacao_arquivo.append(_data_modificacao_arquivo)

                c += 1
                l += 1

            if not row or c == chunksize:
                if c > 0 or l == 0:

                    data = {
                        'referencia_externa': referencia_externa,
                        'entidade_registradora': entidade_registradora,
                        'instituicao_credenciadora_subcredenciadora': instituicao_credenciadora_subcredenciadora,
                        'usuario_final_recebedor': usuario_final_recebedor,
                        'arranjo_pagamento': arranjo_pagamento,
                        'data_liquidacao': data_liquidacao,
                        'titular_unidade_recebivel': titular_unidade_recebivel,
                        'constituicao_unidade_recebivel': constituicao_unidade_recebivel,
                        'valor_constituido_total': valor_constituido_total,
                        'valor_constituido_antecipacao_pre_contratada': valor_constituido_antecipacao_pre_contratada,
                        'valor_bloqueado': valor_bloqueado,
                        'informacoes_pagamento_unidade_recebivel': informacoes_pagamento_unidade_recebivel,
                        'carteira': carteira,
                        'valor_livre': valor_livre,
                        'valor_total_ur': valor_total_ur,
                        'data_hora_ultima_atualizacao_ur': data_hora_ultima_atualizacao_ur,
                        'identificacao_registradora': identificacao_registradora,
                        'identificacao_registradora': identificacao_registradora,
                        'identificacao_participante': identificacao_participante,
                        'nome_arquivo': nome_arquivo,
                        'identificacao_processo_carga_arquivo': identificacao_processo_carga_arquivo,
                        'data_envio_arquivo': data_envio_arquivo,
                        'data_modificacao_arquivo': data_modificacao_arquivo
                    }

                    parquet_file_writer(
                        data_lake_credentials,
                        '{0}/{1}/id={2}/participante={3}/year={4}/month={5}/day={6}/{7}_part{8}.{9}'.format(
                            data_lake_bucket,
                            'registradoras',
                            str(_identificacao_registradora),
                            str(_identificacao_participante).rjust(8, '0'),
                            csv_filename.split('_')[2][:4],
                            csv_filename.split('_')[2][4:6],
                            csv_filename.split('_')[2][6:],
                            os.path.splitext(os.path.basename(csv_filename))[0],
                            str(p).rjust(4, '0'),
                            'parquet'
                        ),
                        data
                    )

                    referencia_externa = []
                    entidade_registradora = []
                    instituicao_credenciadora_subcredenciadora = []
                    usuario_final_recebedor = []
                    arranjo_pagamento = []
                    data_liquidacao = []
                    titular_unidade_recebivel = []
                    constituicao_unidade_recebivel = []
                    valor_constituido_total = []
                    valor_constituido_antecipacao_pre_contratada = []
                    valor_bloqueado = []
                    informacoes_pagamento_unidade_recebivel = []
                    carteira = []
                    valor_livre = []
                    valor_total_ur = []
                    data_hora_ultima_atualizacao_ur = []
                    identificacao_registradora = []
                    identificacao_registradora = []
                    identificacao_participante = []
                    nome_arquivo = []
                    identificacao_processo_carga_arquivo = []
                    data_envio_arquivo = []
                    data_modificacao_arquivo = []

                    c = 0
                    p += 1

            if not row:
                file.close()
                break
    except Exception as e:
        logger.critical(e)
        
################################## PARA RODAR LOCAL UM ARQUIVO POR VEZ ##################################
if __name__ == '__main__':

    data_source_credentials = {
        'access_key_id': 'XXXXXXXXXXXXXX',
        'secret_access_key': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    }

    data_lake_credentials = {
        'access_key_id': 'XXXXXXXXXXXXXX',
        'secret_access_key': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    }

    csv_filename = 'production-project001/CERC-AP005_23399607_20220112_0000001.csv'

    data_lake_bucket = 'data-lake-project001'

    csv_file_reader(
        data_source_credentials,
        csv_filename,
        data_lake_credentials,
        data_lake_bucket
    )