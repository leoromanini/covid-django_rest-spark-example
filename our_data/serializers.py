from .models import Report
from rest_framework import serializers


class ReportSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Report
        fields = '__all__'