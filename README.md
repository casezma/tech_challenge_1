# TechChallenge 01

Este projeto tem como objetivo:

- Criação de API que retorne dados do site referente a vinicultura da EMBRAPA.
- A API deverá alimentar uma base de dados que será utilizada futuramente para um modelo de ML.


# Getting Started

```console
foo@bar:~$ pip install -r requirements.txt
foo@bar:~$ python manage.py makemigrations api
foo@bar:~$ python manage.py migrate
foo@bar:~$ python manage.py runserver 0.0.0.0:8000

```

Chamar o método GET '/api/buscar-dados-embrapa-e-criar-update/' para que os arquivos sejam baixados e a API possa ser consultada.

# Arquitetura

Abaixo está o diagrama com a arquitetura proposta para o Deploy da API

![screenshot](arquitetura.png)


# MVP

Para abrir a Documentação da API:

http://34.31.123.18:8000/api/docs/

