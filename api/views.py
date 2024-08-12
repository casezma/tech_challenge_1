
from api.models import *
from datetime import datetime
from rest_framework.response import Response
from rest_framework import status as http_status
from rest_framework.decorators import api_view

from api.serializer import *

from scripts import run


@api_view(['GET'])
def get_data_from_embraba_and_create_update(request):
    """Cria um objeto chamado atualização que servirá para um 'versionamento' das mesmas.
    Posteriormente roda o método run.
    Após a execução os dados referentes a esta atualizacao são informados
    """
    atualizacao = Atualizacao.objects.create(ts = datetime.now(),status="EM ANDAMENTO")
    run(atualizacao)
    d = {
        "id": atualizacao.id,
        "ts":atualizacao.ts,
        "status":atualizacao.status,
        "detalhes":atualizacao.detalhes
    }
    return Response(data = d)

@api_view(['GET'])
def get_update_state(request,pk):
    """Serve para consultar a atualização passando a chave primaria da mesma."""
    item = Atualizacao.objects.get(id = pk)
    return Response(AtualizacaoSerializer(item).data)



@api_view(['DELETE'])
def delete_update(request,pk):
    """Deleta o update em CASCADE. """
    try:
        Atualizacao.objects.get(id = pk).delete()
        return Response(status = http_status.HTTP_204_NO_CONTENT)
    except Atualizacao.DoesNotExist:
        return Response(d = {"details":f"pk {pk} not found."}, status = http_status.HTTP_404_NOT_FOUND)
    except Exception as ex:
        return Response(d = {"details":f"Unexpected Error. {ex}"}, status = http_status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    
@api_view(['GET'])
def list_table(request,table,pk_atualizacao):
        """Lista todos os dados de uma tabela específica, sendo obrigatório passar o pk de atualização.
           Caso a tabela chamada seja atualização,serão mostradas todas as atualizacoes.
        """
        try:
            choices = ['Producao','Comercializacao','Processamento','Importacao','Exportacao','Atualizacao']
            table = str(table).lower().capitalize()
            if table not in choices:
                return Response({"details":f"table parameter must be one of these: {','.join(choices)}"})
            elif table == "Atualizacao":
                 items = Atualizacao.objects.all()
                 return Response(data = AtualizacaoSerializer(items, many = True).data)
            
            atualizacao = Atualizacao.objects.get(pk = pk_atualizacao)
            items = globals().get(table).objects.filter(atualizacao = atualizacao)
            serializer = globals().get(f"{table}Serializer")(items,many=True)
            return Response(serializer.data)
        except Atualizacao.DoesNotExist:
              return Response({"details":f"Atualizacao object id {pk_atualizacao} does not exists."}, 
                              status = http_status.HTTP_400_BAD_REQUEST)