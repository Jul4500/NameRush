# discord-sniper

Snipe des usernames Discord de N caractères via `POST /applications` (API v10).  
Ordre aléatoire · multi-token · rotation de proxies · reprise automatique · notification webhook.

---

## Fonctionnement

1. Génère tous les usernames possibles sur l'alphabet configuré (défaut : `a-z0-9`, longueur 4 → 1 679 616 combos)
2. Les shuffle dans un ordre aléatoire déterministe (seed sauvegardé → reprise exacte après Ctrl-C)
3. Pour chaque username, tente `POST /applications` avec ton token Discord
4. Si succès → crée le bot (`POST /applications/{id}/bot`), sauvegarde dans `sniped.txt`, envoie un embed sur le webhook

---

## Installation

### Termux (Android)

```bash
pkg update && pkg upgrade -y
pkg install python python-pip unzip git -y

git clone https://github.com/TON_PSEUDO/discord-sniper
cd discord-sniper

pip install -r requirements.txt

cp config.example.json config.json
cp tokens.example.txt tokens.txt
cp proxies.example.txt proxies.txt   # optionnel
```

### Linux / VPS

```bash
git clone https://github.com/TON_PSEUDO/discord-sniper
cd discord-sniper
pip install -r requirements.txt
cp config.example.json config.json
cp tokens.example.txt tokens.txt
```

---

## Configuration

### 1. Tokens — `tokens.txt`

Un token Discord utilisateur par ligne (pas de prefix `Bot `) :

```
MTA4NjY3NzU0NTY0MDExMTY4.XXXXXX.ton_token_ici
MTA4NjY3NzU0NTY0MDExMTY5.XXXXXX.deuxieme_token
```

> Récupère ton token : Discord → F12 → Network → n'importe quelle requête → Header `Authorization`

### 2. Config — `config.json`

| Clé | Défaut | Description |
|-----|--------|-------------|
| `webhook_url` | `""` | URL webhook Discord pour les notifications (vide = désactivé) |
| `alphabet` | `a-z0-9` | Caractères utilisés pour générer les usernames |
| `length` | `4` | Longueur des usernames ciblés |
| `concurrency` | `1` | Workers simultanés |
| `rate_limit_delay` | `5.0` | Secondes entre chaque requête par token |
| `team_id` | `null` | Snowflake d'une team Discord (optionnel) |
| `client_build_number` | `260813` | Build Discord — à mettre à jour périodiquement |

### 3. Proxies — `proxies.txt` (optionnel)

```
http://user:pass@host:port
socks5://user:pass@host:port
```

---

## Utilisation

```bash
# Lancement de base
python sniper.py

# Dry-run (aucun appel API — test de setup)
python sniper.py --dry-run

# 2 workers (nécessite 2 tokens dans tokens.txt)
python sniper.py --threads 2

# Avec proxies
python sniper.py --proxy-file proxies.txt

# Webhook one-shot (sans modifier config.json)
python sniper.py --webhook "https://discord.com/api/webhooks/..."

# Reprendre depuis un index précis
python sniper.py --start-from 840000

# Alphabet custom (minuscules seulement)
python sniper.py --alphabet abcdefghijklmnopqrstuvwxyz

# Forcer une nouvelle session (nouvel ordre aléatoire)
rm state.json && python sniper.py
```

### En arrière-plan (Termux / VPS)

```bash
pkg install tmux -y       # Termux
# ou : apt install tmux   # VPS

tmux new -s sniper
python sniper.py
# Ctrl+B puis D → détache, tourne en fond
# tmux attach -t sniper   → revenir
```

---

## Fichiers générés

| Fichier | Contenu |
|---------|---------|
| `sniped.txt` | Snipes réussis : `USERNAME='abcd'  APP_ID=...  BOT_TOKEN=...` |
| `log.txt` | Debug complet — 400 bodies bruts, 429, backoffs |
| `state.json` | Position + seed de shuffle — reprise automatique au relancement |

---

## Ajustements API

Si Discord change son API, cherche ces tags dans `sniper.py` :

| Tag | Quoi modifier |
|-----|--------------|
| `[ENDPOINT]` | Chemins `POST /applications` et `POST /applications/{id}/bot` |
| `[ERR_CODE]` | Détection des 400 "username taken" — set `TAKEN_SUBS` |
| `[BOT_CREATE]` | Étape de récupération du bot token |
| `[HEADERS]` | Set de headers complet |
| `[BUILD_NUM]` | `client_build_number` — mettre à jour depuis `/api/v9/experiments` |

---

## Notification webhook

L'embed envoyé sur le webhook à chaque snipe :

```
★ Username sniped
Username  │ App ID
  abcd    │  1234567890123456789

Bot Token │ ||MTAxN...token...|| (spoiler — clic pour révéler)
```

---

## Notes

- Le rate limit Discord sur `POST /applications` est ~1 req/5s par token — ne pas descendre `rate_limit_delay` sous `3.0`
- `state.json` sauvegarde la progression à chaque username tenté — Ctrl-C sans perte
- Le shuffle utilise un seed fixe sauvegardé : même ordre garanti à la reprise, zéro doublon
- Mettre à jour `client_build_number` si tu vois des 401 en masse soudains
