# Comment Exécuter RawVaultETL dans IntelliJ IDEA

## ❌ Problème Actuel
```
Error: Unable to initialize main class bronze.RawVaultETL
Caused by: java.lang.NoClassDefFoundError: org/apache/spark/sql/Dataset
```

## ✅ Solutions

### Solution 1 : Recharger le Projet SBT dans IntelliJ (Recommandé)

1. **Ouvrir la fenêtre SBT** :
   - Clic droit sur `build.sbt`
   - Sélectionner "Reload SBT Project"
   
   OU
   
   - Dans la barre latérale droite, cliquer sur l'onglet "sbt"
   - Cliquer sur l'icône "Reload All SBT Projects" (icône circulaire avec flèches)

2. **Attendre la synchronisation** (peut prendre 1-2 minutes)
   - IntelliJ va télécharger toutes les dépendances Spark
   - Les dépendances apparaîtront dans "External Libraries"

3. **Vérifier que Spark est présent** :
   - Ouvrir "Project Structure" (Ctrl+Alt+Shift+S)
   - Aller dans "Libraries"
   - Vérifier que vous voyez "org.apache.spark:spark-core_2.12:3.5.0"

4. **Ré-exécuter RawVaultETL** :
   - Clic droit sur `RawVaultETL.scala`
   - "Run 'RawVaultETL'"

---

### Solution 2 : Exécuter via SBT Terminal (Plus Simple)

Au lieu d'utiliser le bouton "Run" d'IntelliJ, utilisez le terminal SBT :

1. **Ouvrir le Terminal IntelliJ** (Alt+F12)

2. **Exécuter via SBT** :
   ```bash
   sbt "runMain bronze.RawVaultETL --mode full"
   ```

Cette méthode fonctionne toujours car SBT gère lui-même les dépendances.

---

### Solution 3 : Configurer une Run Configuration SBT

1. **Créer une nouvelle Run Configuration** :
   - Menu "Run" → "Edit Configurations..."
   - Cliquer sur "+" → "sbt Task"

2. **Configurer** :
   - Name: `Bronze ETL - Full Mode`
   - Tasks: `runMain bronze.RawVaultETL --mode full`
   - Working directory: `C:\dev\projects\data-vault-modeling-etl`

3. **Sauvegarder et Exécuter** :
   - Cliquer sur "OK"
   - Utiliser cette configuration depuis le menu déroulant Run

---

## 📋 Vérification Rapide

Pour vérifier que Spark est correctement chargé :

1. Ouvrir "Project Structure" (Ctrl+Alt+Shift+S)
2. Aller dans "Libraries"
3. Chercher "spark-core"
4. Si absent, recharger SBT (Solution 1)

---

## 🚀 Commandes Alternatives (Terminal)

Si IntelliJ continue à poser problème, utilisez directement le terminal :

```powershell
# Dans le répertoire du projet
cd C:\dev\projects\data-vault-modeling-etl

# Exécuter l'ETL
sbt "runMain bronze.RawVaultETL --mode full"

# Ou avec entity spécifique
sbt "runMain bronze.RawVaultETL --entity customer --mode full"
```

---

## ✅ Résultat Attendu

Une fois Spark correctement chargé, vous devriez voir :

```
╔════════════════════════════════════════════════════════════════╗
║         DATA VAULT 2.0 - RAW VAULT ETL (BRONZE LAYER)         ║
╚════════════════════════════════════════════════════════════════╝

Configuration:
  Mode: full (full|incremental)
  Entity: all

🚀 Initializing Spark Session with Iceberg & Hive Metastore...
✅ Spark 3.5.0 initialized
...
```

