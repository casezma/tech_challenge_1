
from django.urls import path, include
from api.views import get_data_from_embraba_and_create_update, delete_update, list_table,get_update_state
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView

urlpatterns = [
	path('buscar-dados-embrapa-e-criar-update/',get_data_from_embraba_and_create_update),
    path('deletar-update/<int:pk>/',delete_update),
    path('consultar-update/<int:pk>/',get_update_state),
    path('listar-tabela/<str:table>/<int:pk_atualizacao>/',list_table),
    path('schema/', SpectacularAPIView.as_view(), name='schema'),
    path('docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
]