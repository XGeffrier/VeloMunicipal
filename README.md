# Vélo municipal

À la veille du second tour des élections municipales de 2026 en France, ce projet a pour objectif de mesurer l'évolution des pistes cyclables dans les communes ces dernières années et de le comparer à la couleur politique de la mairie.

Ce projet a été intégralement réalisé grâce à des données ouvertes.

## Sources des données

### Pistes cyclables
- Fichiers : [france-20210708.geojson](https://www.data.gouv.fr/datasets/amenagements-cyclables-france-metropolitaine?resource_id=3c478b0c-e8b5-48fc-a543-9739f2abb4dd) et [france-20260302.geojson](https://www.data.gouv.fr/datasets/amenagements-cyclables-france-metropolitaine?resource_id=142fecf6-4873-4dac-9876-2019d925eaf8)
- Description : Cette base de données contient l'ensemble des aménagements cyclables de France métropolitaine numérisés dans OpenStreetMap et traités par Geovelo afin de les restituer selon le schéma national des aménagements cyclables. Le rattachement se fait par rapport aux codes géographiques des communes du millésime 2022.
- Producteur : [Geovelo](https://geovelo.app/fr/)
- Dates : 8 juillet 2021 et 2 mars 2026
- Notes diverses :
  - Schéma utilisé : [Schéma national des aménagements cyclables](https://doc.transport.data.gouv.fr/type-donnees/amenagements-cyclables/normes-et-standards-schema-national-des-amenagements-cyclables)
  - On pourrait se contenter de la version 2026 et du champ `d_service` indiquant la date de mise en service, mais ce champ est en pratique souvent vide, d'où le choix de comparer les versions 2026 et 2021 (l'idéal aurait été 2020 mais cette donnée est indisponible).

### Forme des communes
- Fichier : [communes-5m.geojson](https://adresse.data.gouv.fr/data/contours-administratifs/latest/geojson/communes-5m.geojson)
- Description : La forme des communes à une précision de 5m.
- Producteur : [Base Adresse Nationale](https://adresse.data.gouv.fr/)
- Date : 2025

### Kilométrage des routes par commune 
- Fichier : [routes-km-communes-france-2.csv](https://www.data.gouv.fr/datasets/kilometrage-des-types-de-routes-repartis-par-communes-1?resource_id=da40f3d7-d2e5-4668-ae42-2ad187f59265)
- Description : Kilométrage des types de routes répartis par communes. Jeu de données produit à partir des données OpenStreetMap française
- Producteur : [Simul'impact](https://www.simul-impact.fr/)
- Date : 27 janvier 2026

### Étiquette politique de la mairie par commune
- Fichier : [communes-enrichiers-couleur-politique_2020.csv](https://www.data.gouv.fr/datasets/communes-enrichies-avec-la-nuance-politique-france?resource_id=ea5d6bc3-37d0-4884-a437-155a90c3e05f)
- Description : Ce jeu de données publié correspond donc à la nuance politique récupérée des résultats des élections municipales aux tours 1 et 2, disponibles sur OpenDataSoft, appliquée à la commune (une nuance par commune).
- Producteur : [Datactivist](https://datactivist.coop/fr/)
- Date : élections de 2020
- Notes diverses :
  - La description détaillée de la méthode utilisée est disponible sur [cette page de data.gouv.fr](https://www.data.gouv.fr/datasets/communes-enrichies-avec-la-nuance-politique-france) et vaut la peine d'être lue.
  - La liste des nuances politiques par acronyme est disponible [ici](https://www.archives-resultats-elections.interieur.gouv.fr/resultats/municipales-2020/nuances.php)

### Population par commune
- Fichier : [ensemble/donnees_communes.csv](https://www.insee.fr/fr/statistiques/8680726?sommaire=8681011)
- Description : Fichier d'ensemble des populations en 2023
- Producteur : [INSEE](https://www.insee.fr)
- Date : recensement de 2023

### Codes postaux
- Fichier : [019HexaSmal.csv](https://www.data.gouv.fr/datasets/base-officielle-des-codes-postaux?resource_id=008a2dda-2c60-4b63-b910-998f6f818089)
- Description : Base officielle des codes postaux
- Producteur : [La Poste](https://www.laposte.fr/)
- Date : 08/02/2026
