from django.db import models


class Report(models.Model):
    location = models.CharField(max_length=255)
    date = models.DateField()
    total_cases = models.IntegerField(null=True)
    new_cases = models.IntegerField(null=True)
    new_cases_smoothed = models.FloatField(null=True)
    total_deaths = models.IntegerField(null=True)
    new_deaths = models.IntegerField(null=True)
    new_deaths_smoothed = models.FloatField(null=True)
    total_cases_per_million = models.FloatField(null=True)
    new_cases_per_million = models.FloatField(null=True)
    new_cases_smoothed_per_million = models.FloatField(null=True)
    total_deaths_per_million = models.FloatField(null=True)
    new_deaths_per_million = models.FloatField(null=True)
    new_deaths_smoothed_per_million = models.FloatField(null=True)
    reproduction_rate = models.FloatField(null=True)

