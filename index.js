// hec_notify_server/index.js
// Serveur de notifications push HEC Connect
//
// Écoute trois collections Firestore :
//   1. conversations/*/messages  → notif directe au destinataire (token FCM)
//   2. announcements             → notif par topic (tous / rôle / niveau / filière)
//   3. schedules                 → notif aux étudiants concernés lors d'un nouveau cours

const express = require('express');
const admin   = require('firebase-admin');

// ── Firebase Admin ─────────────────────────────────────────────────────────────
const serviceAccount = JSON.parse(process.env.GOOGLE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });

const db  = admin.firestore();
const app = express();

// ── Sécurité : désactiver les en-têtes révélateurs ────────────────────────────
app.disable('x-powered-by');

// ── CORS : bloquer les requêtes navigateur (l'app est mobile uniquement) ───────
app.use((req, res, next) => {
  // Log utile pour diagnostic — à retirer en production stable
  if (req.path !== '/') {
    console.log(`[${req.method}] ${req.path} | origin="${req.headers.origin ?? 'none'}" ua="${(req.headers['user-agent'] ?? '').slice(0, 60)}"`);
  }
  // L'app Flutter mobile n'envoie pas d'en-tête Origin.
  // Si Origin est présent → requête depuis un navigateur → bloquer.
  if (req.headers.origin) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  next();
});

app.use(express.json({ limit: '10kb' })); // Limite le body à 10 Ko

// ── Rate limiters (implémentation native, sans dépendance externe) ─────────────
const _rlStore = new Map(); // ip → { count, resetAt }

function makeRateLimiter(maxReq, windowMs, message) {
  return (req, res, next) => {
    const ip  = req.ip || req.socket?.remoteAddress || 'unknown';
    const now = Date.now();
    let   rec = _rlStore.get(ip);

    if (!rec || now > rec.resetAt) {
      _rlStore.set(ip, { count: 1, resetAt: now + windowMs });
      return next();
    }
    rec.count += 1;
    if (rec.count > maxReq) {
      return res.status(429).json(message);
    }
    next();
  };
}

// Validation de code d'accès : 10 tentatives / 15 min par IP
const codeLimiter  = makeRateLimiter(10, 15 * 60 * 1000, { valid: false, error: 'Trop de tentatives. Réessayez dans 15 minutes.' });
// Endpoints admin : 20 requêtes / 5 min par IP
const adminLimiter = makeRateLimiter(20,  5 * 60 * 1000, { error: 'Trop de requêtes. Réessayez plus tard.' });

// ── Health check (keep-alive) ─────────────────────────────────────────────────
app.get('/',     (_req, res) => res.send('HEC Notify Server ✅'));
app.get('/ping', (_req, res) => res.json({ status: 'alive', ts: Date.now() }));

// ── Utilitaire : nettoyer un topic ────────────────────────────────────────────
function cleanTopic(str) {
  return str
    .toLowerCase()
    .replace(/\s+/g, '_')
    .replace(/'/g, '')
    .replace(/é|è|ê|ë/g, 'e')
    .replace(/à|â/g, 'a')
    .replace(/ô/g, 'o')
    .replace(/î/g, 'i')
    .replace(/û/g, 'u')
    .replace(/ç/g, 'c')
    .replace(/[^a-z0-9_-]/g, '');
}

// ── Mapping targetPublic → topic FCM ─────────────────────────────────────────
const TARGET_TO_TOPIC = {
  'tous les utilisateurs':           'tous_les_utilisateurs',
  'etudiants uniquement':            'etudiants_uniquement',
  'enseignants uniquement':          'enseignants_uniquement',
  'administration':                  'administration',
  'staff':                           'staff',
  'bachelor 1':                      'bachelor_1',
  'bachelor 2':                      'bachelor_2',
  'bachelor 3':                      'bachelor_3',
  'master 1':                        'master_1',
  'master 2':                        'master_2',
  'mba':                             'mba',
  'dba':                             'dba',
  'marketing':                       'marketing',
  'finance et comptabilite':         'finance_et_comptabilite',
  'informatique':                    'informatique',
  'droit des affaires':              'droit_des_affaires',
  'gestion des ressources humaines': 'gestion_des_ressources_humaines',
  'communication digitale':          'communication_digitale',
  'management':                      'management',
};

function targetsToTopics(targetPublicStr) {
  if (!targetPublicStr) return ['tous_les_utilisateurs'];
  return targetPublicStr
    .split(',')
    .map(t => {
      const key = cleanTopic(t.trim());
      for (const [k, v] of Object.entries(TARGET_TO_TOPIC)) {
        if (cleanTopic(k) === key) return v;
      }
      return key;
    })
    .filter(Boolean);
}

// Niveaux tronc commun (pas de filière)
const TRONC_COMMUN = ['licence_1', 'master_1', 'bachelor_1'];

// ── 1. Écoute des nouveaux messages (chat) ────────────────────────────────────
function listenMessages() {
  console.log('👂 Écoute : conversations/*/messages');

  return db.collectionGroup('messages').onSnapshot(async (snapshot) => {
    for (const change of snapshot.docChanges()) {
      if (change.type !== 'added') continue;

      const msg    = change.doc.data();
      const msgRef = change.doc.ref;

      const ts         = msg.timestamp?.toDate?.() ?? new Date(0);
      const ageSeconds = (Date.now() - ts.getTime()) / 1000;
      if (ageSeconds > 30) continue;

      const senderId = msg.senderId;
      const type     = msg.type ?? 'text';
      const content  = msg.content ?? '';
      if (!senderId) continue;

      const convRef  = msgRef.parent.parent;
      if (!convRef) continue;
      const convSnap = await convRef.get();
      if (!convSnap.exists) continue;

      const convData         = convSnap.data();
      const participants     = convData.participants ?? [];
      const participantNames = convData.participantNames ?? {};

      const receiverId = participants.find((p) => p !== senderId);
      if (!receiverId) continue;

      const senderName = participantNames[senderId] ?? 'Nouveau message';

      let body;
      switch (type) {
        case 'image': body = '📷 Photo'; break;
        case 'video': body = '🎥 Vidéo'; break;
        case 'voice': body = '🎤 Note vocale'; break;
        default: body = content.length > 100 ? content.slice(0, 97) + '…' : content;
      }

      // Token FCM stocké dans la sous-collection privée (non accessible aux autres users)
      const tokenSnap = await db
        .collection('users').doc(receiverId)
        .collection('private').doc('tokens')
        .get();
      if (!tokenSnap.exists) continue;
      const fcmToken = tokenSnap.data().fcmToken;
      if (!fcmToken) continue;

      try {
        await admin.messaging().send({
          token: fcmToken,
          notification: { title: senderName, body },
          android: {
            priority: 'high',
            notification: { sound: 'default', channelId: 'hec_messages' },
          },
          apns: { payload: { aps: { sound: 'default', badge: 1 } } },
          data: { convId: convRef.id, senderId, type },
        });
        console.log(`✅ Chat → ${receiverId} | ${senderName}: ${body}`);
      } catch (err) {
        console.error(`❌ FCM chat error pour ${receiverId}:`, err.message);
      }
    }
  }, (err) => console.error('❌ Firestore messages error:', err));
}

// ── 2. Écoute des nouvelles annonces ─────────────────────────────────────────
function listenAnnouncements() {
  console.log('👂 Écoute : announcements');

  const startedAt = new Date();

  return db.collection('announcements').onSnapshot(async (snapshot) => {
    for (const change of snapshot.docChanges()) {
      if (change.type !== 'added') continue;

      const data = change.doc.data();

      const createdAt = data.createdAt?.toDate?.() ?? new Date(0);
      if (createdAt <= startedAt) continue;

      if (!data.sendNotification) continue;

      const title        = data.title ?? 'Nouvelle annonce';
      const body         = data.content
        ? (data.content.length > 120 ? data.content.slice(0, 117) + '…' : data.content)
        : '';
      const targetPublic = data.targetPublic ?? 'Tous les utilisateurs';
      const category     = data.category ?? 'Général';

      const topics = targetsToTopics(targetPublic);
      console.log(`📢 Annonce "${title}" → topics: ${topics.join(', ')}`);

      for (const topic of topics) {
        try {
          await admin.messaging().send({
            topic,
            notification: { title: `📢 ${title}`, body },
            android: {
              priority: 'high',
              notification: { sound: 'default', channelId: 'hec_announcements', tag: change.doc.id },
            },
            apns: { payload: { aps: { sound: 'default', badge: 1 } } },
            data: { type: 'announcement', announcementId: change.doc.id, category },
          });
          console.log(`  ✅ Envoyé → topic "${topic}"`);
        } catch (err) {
          console.error(`  ❌ FCM topic "${topic}" error:`, err.message);
        }
      }
    }
  }, (err) => console.error('❌ Firestore announcements error:', err));
}

// ── 3. Écoute des nouveaux cours (emploi du temps) ───────────────────────────
function listenSchedules() {
  console.log('👂 Écoute : schedules');

  const startedAt = new Date();

  return db.collection('schedules').onSnapshot(async (snapshot) => {
    for (const change of snapshot.docChanges()) {
      if (change.type !== 'added') continue;

      const data = change.doc.data();

      const createdAt = data.createdAt?.toDate?.() ?? new Date(0);
      if (createdAt <= startedAt) continue;

      const niveau    = data.niveau    ?? '';
      const filiere   = data.filiere   ?? '';
      const matiere   = data.matiere   ?? 'cours';
      const jour      = data.jour      ?? '';
      const slot      = data.slot      ?? '';
      const profNom   = data.professeurNom ?? '';

      if (!niveau) continue;

      const niveauTopic = cleanTopic(niveau);
      const troncCommun = TRONC_COMMUN.includes(niveauTopic);

      const title = `📅 Nouveau cours — ${matiere}`;
      const body  = `${jour} ${slot}${profNom ? ' • ' + profNom : ''}`;

      // Tronc commun → notif par niveau (ex: licence_1)
      // Avec filière → notif par filière (plus ciblé, ex: marketing)
      const topics = troncCommun
        ? [niveauTopic]
        : (filiere ? [cleanTopic(filiere)] : [niveauTopic]);

      console.log(`📅 Nouveau cours "${matiere}" → ${niveau}${filiere ? ' / ' + filiere : ' (tronc commun)'}`);

      for (const topic of topics) {
        try {
          await admin.messaging().send({
            topic,
            notification: { title, body },
            android: {
              priority: 'high',
              notification: { sound: 'default', channelId: 'hec_announcements' },
            },
            apns: { payload: { aps: { sound: 'default', badge: 1 } } },
            data: { type: 'schedule', niveau, filiere, matiere },
          });
          console.log(`  ✅ Cours notifié → topic "${topic}"`);
        } catch (err) {
          console.error(`  ❌ FCM schedule topic "${topic}" error:`, err.message);
        }
      }
    }
  }, (err) => console.error('❌ Firestore schedules error:', err));
}

// ── 4. Endpoint : valider un code d'accès (prof / admin) ─────────────────────
// Le corps attendu : { "type": "teacher" | "Fondateur" | "Directeur" | ..., "code": "..." }
// Les codes sont dans la variable d'env REGISTRATION_CODES (JSON encodé)
// ex: { "teacher": "MON_CODE_PROF", "Fondateur": "MON_CODE_FOND", ... }
app.post('/validateCode', codeLimiter, (req, res) => {
  const { type, code } = req.body;
  if (!type || !code) return res.json({ valid: false });

  let codes;
  try {
    codes = JSON.parse(process.env.REGISTRATION_CODES || '{}');
  } catch {
    console.error('❌ REGISTRATION_CODES malformé');
    return res.status(500).json({ valid: false, error: 'Server config error' });
  }

  const expected = codes[type];
  if (!expected) {
    console.warn(`⚠️  validateCode: type inconnu "${type}"`);
    return res.json({ valid: false });
  }

  const valid = code.trim() === expected.trim();
  console.log(`🔐 validateCode type="${type}" → ${valid ? '✅ valide' : '❌ invalide'}`);
  res.json({ valid });
});

// ── 5. Endpoint : désactiver / réactiver un compte Firebase Auth ──────────────
app.post('/setUserDisabled', adminLimiter, async (req, res) => {
  const { uid, disabled, secret } = req.body;

  if (secret !== process.env.ADMIN_SECRET) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  if (!uid) {
    return res.status(400).json({ error: 'uid requis' });
  }

  try {
    await admin.auth().updateUser(uid, { disabled: Boolean(disabled) });
    console.log(`${disabled ? '🔒' : '🔓'} Auth ${disabled ? 'désactivé' : 'réactivé'} : ${uid}`);
    res.json({ success: true });
  } catch (err) {
    console.error(`❌ setUserDisabled error:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── 7. Endpoint : supprimer un utilisateur Firebase Auth ─────────────────────
app.post('/deleteUser', adminLimiter, async (req, res) => {
  const { uid, secret } = req.body;

  if (secret !== process.env.ADMIN_SECRET) {
    return res.status(403).json({ error: 'Unauthorized' });
  }
  if (!uid) {
    return res.status(400).json({ error: 'uid requis' });
  }

  try {
    await admin.auth().deleteUser(uid);
    console.log(`🗑️ Auth supprimé : ${uid}`);
    res.json({ success: true });
  } catch (err) {
    console.error(`❌ deleteUser error:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Gestion des listeners avec reconnexion automatique ────────────────────────
let _unsubscribers = [];

function startListeners() {
  // Arrêter les anciens listeners avant de redémarrer
  for (const unsub of _unsubscribers) {
    try { unsub(); } catch (e) { /* ignore */ }
  }
  _unsubscribers = [];

  console.log('🔄 Démarrage des listeners Firestore...');
  _unsubscribers.push(listenMessages());
  _unsubscribers.push(listenAnnouncements());
  _unsubscribers.push(listenSchedules());
}

// ── Démarrage ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 HEC Notify Server — port ${PORT}`);
  startListeners();
  // Renouveler les listeners toutes les 20 min (évite la stagnation sur Render free)
  setInterval(startListeners, 20 * 60 * 1000);
});
