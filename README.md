# enedisdata_import
A simple python script to import ENEDIS data consumptions

Todo :
- Ratelimit (5 hits per second max)


Limitations :

- consumption_load_curve & production_load_curve => maximum de 24 mois et 15 jours avant la date d’appel.
- daily_consumption_max_power, daily_consumption, daily_production => maximum de 24 mois et 15 jours avant la date d’appel.
