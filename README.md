Décompresser le fichier exe.zip :
- Télécharger nodejs version 18.20.7
verifier en exécutant :
node -v
npm -v
- Télecharger Python version 3.13
Vérifier installation:
py --version  
pip --version
- Télécharger Grafana
- Installer Angular version 17.3.13: npm install -g @angular/cli@17.3.13 puis npm install
Vérifier l'installation: ng version
- Télécharger Prometheus at alertmanager via le lien: https://prometheus.io/download/
- Déplacer  alertmanager.exe, promtool.exe, amtool.exe et prometheus.exe dans un dossier "monitoring" dans C:
- Déplacer les fichier alert_rules.yml, alertmanager.yml et prometheus.yml depuis le projet vers le dossier monitoring
- Lancer le frontend dans emplacement Detection-Anomalies/frontend : ng serve -o
- Naviguer sur http://localhost:4200/
- Exécuter dans Detection-Anomalies/backend : pip install -r requirements.txt
- Lancer le programme dans l'emplacement Detection-Anomalies : py -m backend.app
- Lancer le serveur Prometheus dans l'emplacement du dossier monitoring:  ./prometheus --config.file=prometheus.yml --storage.tsdb.path=./data
- Lancer le serveur Alertmanager dans l'emplacement du dossier monitoring:  ./alertmanager --config.file=alertmanager.yml

- Créer un dashboard Grafana et configurer le dashboard