from django.db import models

# Create your models here.
class Atualizacao(models.Model):
    ts = models.DateTimeField()
    status = models.TextField()
    detalhes = models.TextField(null=True)

class Producao(models.Model):
    atualizacao = models.ForeignKey(to = Atualizacao, on_delete=models.CASCADE)
    produto = models.TextField()
    ano = models.IntegerField()
    quantidade_litros = models.DecimalField(decimal_places=2,max_digits=20)

class Processamento(models.Model):
    atualizacao = models.ForeignKey(to = Atualizacao, on_delete=models.CASCADE)
    classificacao = models.TextField()
    cultivar = models.TextField()
    ano = models.IntegerField()
    quantidade_kg = models.DecimalField(decimal_places=2,max_digits=20)

class Comercializacao(models.Model):
    atualizacao = models.ForeignKey(to = Atualizacao, on_delete=models.CASCADE)
    produto = models.TextField()
    ano =   models.IntegerField()
    quantidade_litros = models.DecimalField(decimal_places=2,max_digits=20)

class Importacao(models.Model):
    atualizacao = models.ForeignKey(to = Atualizacao, on_delete=models.CASCADE)
    classificacao = models.TextField()
    pais = models.TextField()
    ano = models.IntegerField()
    quantidade = models.DecimalField(decimal_places=2,max_digits=20)
    valor_dolares = models.DecimalField(decimal_places=2,max_digits=20)

class Exportacao(models.Model):
    atualizacao = models.ForeignKey(to = Atualizacao, on_delete=models.CASCADE)
    classificacao = models.TextField()
    pais = models.TextField()
    ano = models.IntegerField()
    quantidade = models.DecimalField(decimal_places=2,max_digits=20)
    valor_dolares = models.DecimalField(decimal_places=2,max_digits=20)


