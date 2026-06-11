// hec_notify_server/index.js
// Serveur de notifications push HEC Connect
//
// Écoute deux collections Firestore :
//   1. conversations/*/messages  → notif directe au destinataire (token FCM)
//   2. announcements             → notif par topic (tous / rôle / niveau / filière)

const express = require('express');
const admin   = require('firebase-admin');

// ── Firebase Admin ─────────────────────────────────────────────────────────────
const serviceAccount = JSON.parse(process.env.GOOGLE_CREDENTIALS);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });

const db  = admin.firestore();
const app = express();
app.use(express.json());

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/', (_req, res) => res.send('HEC Notify Server ✅'));

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

// ── 1. Écoute des nouveaux messages (chat) ────────────────────────────────────
function listenMessages() {
  console.log('👂 Écoute : conversations/*/messages');

  db.collectionGroup('messages').onSnapshot(async (snapshot) => {
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

      const userSnap = await db.collection('users').doc(receiverId).get();
      if (!userSnap.exists) continue;
      const fcmToken = userSnap.data().fcmToken;
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

  db.collection('announcements').onSnapshot(async (snapshot) => {
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

// ── 3. Endpoint : supprimer un utilisateur Firebase Auth ─────────────────────
app.post('/deleteUser', async (req, res) => {
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

// ── Démarrage ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 HEC Notify Server — port ${PORT}`);
  listenMessages();
  listenAnnouncements();
});
