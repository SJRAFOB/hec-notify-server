// hec_notify_server/index.js
// Serveur de notifications push HEC Connect
// Structure Firestore :
//   conversations/{convId}/messages/{msgId}
//     → senderId, type, content, timestamp
//   conversations/{convId}
//     → participants: [uid1, uid2], participantNames: {uid: name}
//   users/{uid}
//     → fcmToken: string

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

// ── Écoute des nouveaux messages ──────────────────────────────────────────────
function startListening() {
  console.log('👂 Écoute Firestore : conversations/*/messages');

  db.collectionGroup('messages').onSnapshot(async (snapshot) => {
    for (const change of snapshot.docChanges()) {
      if (change.type !== 'added') continue;

      const msg    = change.doc.data();
      const msgRef = change.doc.ref;

      // Ignorer les anciens messages (présents au démarrage)
      const ts        = msg.timestamp?.toDate?.() ?? new Date(0);
      const ageSeconds = (Date.now() - ts.getTime()) / 1000;
      if (ageSeconds > 30) continue;

      const senderId = msg.senderId;
      const type     = msg.type ?? 'text';
      const content  = msg.content ?? '';

      if (!senderId) continue;

      // Remonter au document conversation
      const convRef = msgRef.parent.parent;
      if (!convRef) continue;

      const convSnap = await convRef.get();
      if (!convSnap.exists) continue;

      const convData        = convSnap.data();
      const participants    = convData.participants ?? [];
      const participantNames = convData.participantNames ?? {};

      // Le destinataire = l'autre participant
      const receiverId = participants.find((p) => p !== senderId);
      if (!receiverId) continue;

      // Nom de l'expéditeur
      const senderName = participantNames[senderId] ?? 'Nouveau message';

      // Corps de la notification
      let body;
      switch (type) {
        case 'image': body = '📷 Photo'; break;
        case 'video': body = '🎥 Vidéo'; break;
        case 'voice': body = '🎤 Note vocale'; break;
        default:      body = content.length > 100 ? content.slice(0, 97) + '…' : content;
      }

      // Token FCM du destinataire
      const userSnap = await db.collection('users').doc(receiverId).get();
      if (!userSnap.exists) continue;
      const fcmToken = userSnap.data().fcmToken;
      if (!fcmToken) continue;

      // Envoi FCM
      try {
        await admin.messaging().send({
          token: fcmToken,
          notification: { title: senderName, body },
          android: {
            priority: 'high',
            notification: { sound: 'default', channelId: 'hec_messages' },
          },
          apns: {
            payload: { aps: { sound: 'default', badge: 1 } },
          },
          data: {
            convId:   convRef.id,
            senderId: senderId,
            type:     type,
          },
        });
        console.log(`✅ Notif → ${receiverId} | ${senderName}: ${body}`);
      } catch (err) {
        console.error(`❌ FCM error pour ${receiverId}:`, err.message);
      }
    }
  }, (err) => {
    console.error('❌ Firestore listener error:', err);
  });
}

// ── Démarrage ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Port ${PORT}`);
  startListening();
});
