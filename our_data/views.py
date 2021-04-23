from django.shortcuts import render, HttpResponse
from .models import Report
from .spark_funcs import process_json_data, get_json_covid_our_data
from .serializers import ReportSerializer
from rest_framework import viewsets
from rest_framework import permissions


class ReportViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows reports to be viewed.
    """
    queryset = Report.objects.all().order_by('location')
    serializer_class = ReportSerializer
    permission_classes = [permissions.IsAuthenticated]
    filterset_fields = ['location', 'date']


def update_database(request):
    """
    API endpoint that executes database update.
    """
    json_data = get_json_covid_our_data()
    df = process_json_data(json_data)

    for row in df.rdd.collect():
        report = Report(date=row.date,
                        location=row.location,
                        total_cases=row.total_cases,
                        new_cases=row.new_cases,
                        new_cases_smoothed=row.new_cases_smoothed,
                        total_deaths=row.total_deaths,
                        new_deaths=row.new_deaths,
                        new_deaths_smoothed=row.new_deaths_smoothed,
                        total_cases_per_million=row.total_cases_per_million,
                        new_cases_per_million=row.new_cases_per_million,
                        new_cases_smoothed_per_million=row.new_cases_smoothed_per_million,
                        total_deaths_per_million=row.total_deaths_per_million,
                        new_deaths_per_million=row.new_deaths_per_million,
                        new_deaths_smoothed_per_million=row.new_deaths_smoothed_per_million,
                        reproduction_rate=row.reproduction_rate
                        )

        if not Report.objects.filter(location=report.location, date=report.date):
            report.save()

    return HttpResponse(b'DONE')
