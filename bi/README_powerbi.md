# Rapport Power BI — Sport Data Solution

Documentation du fichier `bi/sport_data_solution_avantages.pbix`.

## 1. Connexion PostgreSQL (Lecture Seule RGPD)

- **Connecteur :** Base de données PostgreSQL (mode **Importer**)
- **Serveur :** `127.0.0.1:5433` | **Base :** `sds_platform`
- **Compte SQL :** `sds_bi_reader` (mot de passe stocké dans `SDS_BI_READER_PASSWORD` au sein de `.env`)
- **Sécurité :** droits `SELECT` limités aux schémas `gold_mart` et `ops_monitoring` (aucun accès aux tables nominatives `hr_restricted` ni aux transactions brutes `oltp_sports`).

## 2. Tables Importées et Modèle

| Table / Vue | Lignes | Usage |
|---|---:|---|
| `gold_mart.fact_employee_benefits` | 161 | Éligibilité et calcul par salarié pseudonymisé (`employee_key`) |
| `gold_mart.agg_benefits_by_bu` | 5 | Synthèse par Business Unit |
| `gold_mart.agg_sports_monthly` | 175 | Volumes mensuels d'activités par discipline |
| `ops_monitoring.v_pipeline_health_summary` | 8 | Statut et durée des exécutions du pipeline |
| `ops_monitoring.v_quality_layer_summary` | 4 | Résultat des 22 contrôles Soda Core par couche |

- **Relation active :** `gold_mart agg_benefits_by_bu[business_unit]` (1) vers `gold_mart fact_employee_benefits[business_unit]` (*).
- **Paramètre What-If :** `Simulation Taux Prime` (de `2 %` à `8 %`, pas de `1 %`, valeur par défaut `5 %`).
- **Mesures DAX :** définies dans `bi/mesures_dax.dax` (`Budget Prime Actuel (EUR)`, `Budget Prime Simule (EUR)`, `Ecart Simulation (EUR)`).

## 3. Structure des 3 Onglets

1. **Budget et Simulation :** curseur `Simulation Taux Prime`, filtre `business_unit`, cartes budgétaires (`168 824,50 €` à 5 %, écart simulé, `245` jours bien-être), histogramme comparatif et tableau récapitulatif par Business Unit.
2. **Sport et Mobilité :** compteurs clés (`1 723` activités sur 12 mois, `67` éligibles mobilité, `1` anomalie trajet), historique mensuel, classement par discipline et table filtrée sur l'anomalie déclarative bloquée à `0,00 €`.
3. **Qualité et Pipeline :** cartes Soda Core (`22 / 22` contrôles validés), taux de conformité à `100 %` par couche et journal de santé des 8 flux techniques.

## 4. Rafraîchissement des Données

```powershell
uv run python scripts/inject_live_activity.py --sport "Course à pied" --duration-minutes 45 --distance-km 8.5 --comment "Sortie midi"
uv run python scripts/recompute_gold_kpis.py --refresh-bronze-silver
```

Cliquer ensuite sur **Actualiser** dans le ruban **Accueil** de Power BI Desktop.