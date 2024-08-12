from api.models import (
    Producao,Processamento,Comercializacao,Importacao,Exportacao,Atualizacao)
from rest_framework import serializers

class ProducaoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Producao
        fields = '__all__'

class ProcessamentoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Processamento
        fields = '__all__'

class ComercializacaoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Comercializacao
        fields = '__all__'

class ImportacaoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Importacao
        fields = '__all__'

class ExportacaoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Exportacao
        fields = '__all__'

class AtualizacaoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Atualizacao
        fields = '__all__'
