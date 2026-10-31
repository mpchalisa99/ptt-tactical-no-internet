import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:audioplayers/audioplayers.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:network_info_plus/network_info_plus.dart';
import 'package:provider/provider.dart';
import 'package:record/record.dart';
import 'package:udp/udp.dart';

/// ============================================================================
/// 1. ENTRY POINT UTAMA (main) & INISIALISASI MULTIPROVIDER
/// ============================================================================
void main() async {
  WidgetsFlutterBinding.ensureInitialized();

  await SystemChrome.setPreferredOrientations([DeviceOrientation.portraitUp]);
  SystemChrome.setSystemUIOverlayStyle(
    const SystemUiOverlayStyle(
      statusBarColor: Colors.transparent,
      statusBarIconBrightness: Brightness.light,
    ),
  );

  // Inisialisasi service Audio & Jaringan UDP LAN Hotspot
  final audioService = AudioService();
  final udpNetworkService = UdpNetworkService();

  // Hubungkan callback paket suara UDP masuk (Port 50001) ke pemutar PCM
  udpNetworkService.onAudioPacketReceived = (Uint8List pcmBytes, String senderIp) {
    audioService.playAudioChunk(pcmBytes);
  };

  // Jalankan deteksi IP lokal & buka socket UDP (Port 50000 & Port 50001)
  await udpNetworkService.initialize();

  runApp(
    MultiProvider(
      providers: [
        ChangeNotifierProvider<UdpNetworkService>.value(
          value: udpNetworkService,
        ),
        Provider<AudioService>.value(
          value: audioService,
        ),
      ],
      child: const DiskomlekauTacticalPttApp(),
    ),
  );
}

/// ============================================================================
/// 2. MODEL DATA ANGGOTA (PeerMember)
/// ============================================================================
class PeerMember {
  final String id;
  String callsign;
  final String ipAddress;
  final int channel;
  bool isTargeted;
  DateTime lastSeen;
  int lastPingMs;

  PeerMember({
    required this.id,
    required this.callsign,
    required this.ipAddress,
    required this.channel,
    this.isTargeted = true,
    this.lastPingMs = 24,
    DateTime? lastSeen,
  }) : lastSeen = lastSeen ?? DateTime.now();

  /// Factory untuk memproses data JSON dari paket UDP Broadcast Auto-Discovery
  factory PeerMember.fromJson(Map<String, dynamic> json) {
    return PeerMember(
      id: (json['id'] ?? json['ip'] ?? 'UNKNOWN').toString(),
      callsign: (json['callsign'] ?? 'PEER').toString().toUpperCase(),
      ipAddress: (json['ip'] ?? '0.0.0.0').toString(),
      channel: (json['ch'] as num?)?.toInt() ?? 12,
      isTargeted: json['isTargeted'] as bool? ?? true,
      lastPingMs: (json['ping'] as num?)?.toInt() ?? 24,
      lastSeen: DateTime.now(),
    );
  }

  Map<String, dynamic> toJson() => {
        'id': id,
        'callsign': callsign,
        'ip': ipAddress,
        'ch': channel,
        'isTargeted': isTargeted,
        'ping': lastPingMs,
      };
}

/// ============================================================================
/// 3. UDP NETWORK SERVICE (Auto-Discovery Port 50000 & Audio Stream Port 50001)
/// ============================================================================
class UdpNetworkService extends ChangeNotifier {
  static const int discoveryPort = 50000;
  static const int audioPort = 50001;
  static const String masterDefaultIp = '192.168.43.1';

  UDP? _discoverySocket;
  UDP? _audioSocket;
  Timer? _heartbeatTimer;
  Timer? _pruneTimer;

  String _localIp = masterDefaultIp;
  String _myCallsign = 'YUDHI';
  final String _deviceId = 'NODE-${DateTime.now().millisecondsSinceEpoch % 10000}';
  int _currentChannel = 12;
  bool _isMaster = true;
  bool _talkAll = true;
  bool _isTransmitting = false;
  int _lastPingMs = 24;
  int _pingDeltaMs = 4;
  int _txSequence = 0;

  /// Daftar anggota online yang ditemukan via UDP Broadcast (key: IP Address)
  final Map<String, PeerMember> _peers = {};

  /// Callback saat paket suara PCM diterima dari perangkat lain
  void Function(Uint8List pcmBytes, String senderIp)? onAudioPacketReceived;

  String get localIp => _localIp;
  String get myCallsign => _myCallsign;
  int get currentChannel => _currentChannel;
  bool get isMaster => _isMaster;
  bool get talkAll => _talkAll;
  bool get isTransmitting => _isTransmitting;
  int get lastPingMs => _lastPingMs;
  int get pingDeltaMs => _pingDeltaMs;
  bool get isHighLatency => _lastPingMs > 150;

  /// Daftar anggota aktif pada channel yang sama
  List<PeerMember> get activeChannelPeers => _peers.values
      .where((p) => p.channel == _currentChannel)
      .toList()
    ..sort((a, b) => a.callsign.compareTo(b.callsign));

  /// Daftar anggota yang menjadi target pengiriman suara Unicast
  List<PeerMember> get targetedPeers => activeChannelPeers
      .where((p) => _talkAll || p.isTargeted)
      .toList();

  /// Inisialisasi IP lokal Hotspot dan buka socket UDP Port 50000 & 50001
  Future<void> initialize() async {
    await _detectLocalHotspotIp();
    await _bindDiscoverySocket();
    await _bindAudioSocket();

    // Kirim heartbeat UDP Broadcast setiap 1.5 detik untuk Auto-Discovery
    _heartbeatTimer?.cancel();
    _heartbeatTimer = Timer.periodic(
      const Duration(milliseconds: 1500),
      (_) => _sendDiscoveryAnnouncement(),
    );

    // Hapus peer yang tidak mengirim sinyal > 7 detik (putus koneksi WiFi)
    _pruneTimer?.cancel();
    _pruneTimer = Timer.periodic(
      const Duration(seconds: 3),
      (_) => _pruneStalePeers(),
    );

    await _sendDiscoveryAnnouncement();
  }

  /// Mendeteksi IP lokal perangkat di jaringan WiFi Hotspot (mis. 192.168.43.x)
  Future<void> _detectLocalHotspotIp() async {
    try {
      final info = NetworkInfo();
      String? wifiIp = await info.getWifiIP();

      // Fallback scanning NetworkInterface jika HP ini adalah Host Hotspot (ap0/wlan0)
      if (wifiIp == null || wifiIp.isEmpty || wifiIp == '0.0.0.0') {
        final interfaces = await NetworkInterface.list(
          type: InternetAddressType.IPv4,
          includeLoopback: false,
        );
        for (final iface in interfaces) {
          for (final addr in iface.addresses) {
            if (addr.address.startsWith('192.168.')) {
              wifiIp = addr.address;
              break;
            }
          }
        }
      }

      _localIp = wifiIp ?? masterDefaultIp;
      // HP Master (Host Hotspot) di Android umumnya memiliki IP berakhiran .1
      _isMaster = _localIp.endsWith('.1');
      notifyListeners();
    } catch (e) {
      debugPrint('Gagal deteksi IP WiFi Hotspot, gunakan default: $e');
      _localIp = masterDefaultIp;
      _isMaster = true;
    }
  }

  /// Membuka socket UDP Port 50000 untuk mendengarkan Broadcast Auto-Discovery & Ping
  Future<void> _bindDiscoverySocket() async {
    _discoverySocket?.close();
    _discoverySocket = await UDP.bind(Endpoint.any(port: const Port(discoveryPort)));

    _discoverySocket!.asStream().listen((Datagram? datagram) {
      if (datagram == null) return;
      final senderIp = datagram.address.address;
      if (senderIp == _localIp) return;

      try {
        final Map<String, dynamic> data = jsonDecode(utf8.decode(datagram.data));
        final String type = (data['type'] ?? '').toString();

        if (type == 'ANNOUNCE') {
          final incoming = PeerMember.fromJson(data);
          final existing = _peers[senderIp];
          if (existing != null) {
            incoming.isTargeted = existing.isTargeted;
            incoming.lastPingMs = existing.lastPingMs;
          }
          _peers[senderIp] = incoming;
          notifyListeners();
        } else if (type == 'PING') {
          _sendPong(senderIp, (data['ts'] as num).toInt());
        } else if (type == 'PONG') {
          final sentTs = (data['ts'] as num).toInt();
          final rtt = DateTime.now().millisecondsSinceEpoch - sentTs;
          _pingDeltaMs = rtt - _lastPingMs;
          _lastPingMs = rtt.clamp(1, 999);
          if (_peers.containsKey(senderIp)) {
            _peers[senderIp]!.lastPingMs = _lastPingMs;
          }
          notifyListeners();
        }
      } catch (e) {
        debugPrint('Paket UDP Discovery tidak valid dari $senderIp: $e');
      }
    });
  }

  /// Mengirim paket ANNOUNCE ke alamat Broadcast Subnet Hotspot & Global Broadcast
  Future<void> _sendDiscoveryAnnouncement() async {
    if (_discoverySocket == null) return;

    final bytes = utf8.encode(
      jsonEncode({
        'type': 'ANNOUNCE',
        'id': _deviceId,
        'callsign': _myCallsign,
        'ip': _localIp,
        'ch': _currentChannel,
        'ping': _lastPingMs,
      }),
    );

    final parts = _localIp.split('.');
    final subnetBroadcast = parts.length == 4
        ? '${parts[0]}.${parts[1]}.${parts[2]}.255'
        : '192.168.43.255';

    try {
      await _discoverySocket!.send(
        bytes,
        Endpoint.unicast(
          InternetAddress(subnetBroadcast),
          port: const Port(discoveryPort),
        ),
      );
      await _discoverySocket!.send(
        bytes,
        Endpoint.broadcast(port: const Port(discoveryPort)),
      );
    } catch (e) {
      debugPrint('Gagal mengirim UDP broadcast discovery: $e');
    }
  }

  /// Membuka socket UDP Port 50001 untuk menerima stream paket suara PCM mentah
  Future<void> _bindAudioSocket() async {
    _audioSocket?.close();
    _audioSocket = await UDP.bind(Endpoint.any(port: const Port(audioPort)));

    _audioSocket!.asStream().listen((Datagram? datagram) {
      if (datagram == null || datagram.data.length <= 6) return;
      final senderIp = datagram.address.address;
      if (senderIp == _localIp) return;

      final bytes = datagram.data;
      final packetChannel = bytes[0];
      if (packetChannel != _currentChannel) return;

      final pcmPayload = Uint8List.sublistView(bytes, 6);
      onAudioPacketReceived?.call(pcmPayload, senderIp);
    });
  }

  /// Mengirimkan potongan byte suara PCM 16-bit ke anggota aktif via UDP Port 50001
  Future<void> sendRawAudioChunk(Uint8List pcmChunk) async {
    if (_audioSocket == null || pcmChunk.isEmpty) return;

    _txSequence = (_txSequence + 1) & 0xFFFF;

    // Header 6-Byte: [Channel(1B), Flags(1B), Sequence(2B), Length(2B)]
    final header = ByteData(6);
    header.setUint8(0, _currentChannel);
    header.setUint8(1, (_isMaster ? 1 : 0) | (_talkAll ? 2 : 0));
    header.setUint16(2, _txSequence, Endian.big);
    header.setUint16(4, pcmChunk.length, Endian.big);

    final packet = Uint8List(6 + pcmChunk.length);
    packet.setRange(0, 6, header.buffer.asUint8List());
    packet.setRange(6, packet.length, pcmChunk);

    final targets = targetedPeers;
    if (targets.isEmpty) {
      // Fallback ke subnet broadcast jika tabel peer masih kosong
      final parts = _localIp.split('.');
      final subnetBroadcast = parts.length == 4
          ? '${parts[0]}.${parts[1]}.${parts[2]}.255'
          : '192.168.43.255';
      try {
        await _audioSocket!.send(
          packet,
          Endpoint.unicast(
            InternetAddress(subnetBroadcast),
            port: const Port(audioPort),
          ),
        );
      } catch (_) {}
      return;
    }

    for (final peer in targets) {
      try {
        await _audioSocket!.send(
          packet,
          Endpoint.unicast(
            InternetAddress(peer.ipAddress),
            port: const Port(audioPort),
          ),
        );
      } catch (e) {
        debugPrint('Gagal kirim paket audio ke ${peer.ipAddress}: $e');
      }
    }
  }

  /// Mengirim paket PING ke seluruh peer aktif untuk memperbarui nilai latensi
  Future<void> refreshPing() async {
    if (_discoverySocket == null) return;
    final now = DateTime.now().millisecondsSinceEpoch;
    final bytes = utf8.encode(jsonEncode({'type': 'PING', 'ts': now}));

    for (final peer in _peers.values) {
      try {
        await _discoverySocket!.send(
          bytes,
          Endpoint.unicast(
            InternetAddress(peer.ipAddress),
            port: const Port(discoveryPort),
          ),
        );
      } catch (_) {}
    }
  }

  Future<void> _sendPong(String targetIp, int originalTs) async {
    if (_discoverySocket == null) return;
    final bytes = utf8.encode(jsonEncode({'type': 'PONG', 'ts': originalTs}));
    try {
      await _discoverySocket!.send(
        bytes,
        Endpoint.unicast(
          InternetAddress(targetIp),
          port: const Port(discoveryPort),
        ),
      );
    } catch (_) {}
  }

  void _pruneStalePeers() {
    final now = DateTime.now();
    final beforeCount = _peers.length;
    _peers.removeWhere((_, peer) => now.difference(peer.lastSeen).inSeconds > 7);
    if (_peers.length != beforeCount) {
      notifyListeners();
    }
  }

  void setChannel(int newChannel) {
    _currentChannel = newChannel.clamp(1, 99);
    _sendDiscoveryAnnouncement();
    notifyListeners();
  }

  void setCallsign(String newName) {
    if (newName.trim().isEmpty) return;
    _myCallsign = newName.trim().toUpperCase();
    _sendDiscoveryAnnouncement();
    notifyListeners();
  }

  void toggleTalkAll() {
    _talkAll = !_talkAll;
    if (_talkAll) {
      for (final p in _peers.values) {
        p.isTargeted = true;
      }
    }
    notifyListeners();
  }

  void togglePeerTarget(String ip) {
    final peer = _peers[ip];
    if (peer != null) {
      peer.isTargeted = !peer.isTargeted;
      _talkAll = _peers.values.every((p) => p.isTargeted);
      notifyListeners();
    }
  }

  void setTransmitting(bool value) {
    _isTransmitting = value;
    _sendDiscoveryAnnouncement();
    notifyListeners();
  }

  @override
  void dispose() {
    _heartbeatTimer?.cancel();
    _pruneTimer?.cancel();
    _discoverySocket?.close();
    _audioSocket?.close();
    super.dispose();
  }
}

/// ============================================================================
/// 4. AUDIO SERVICE (Perekam Mic Raw PCM 16-bit 16kHz & Pemutar Stream UDP)
/// ============================================================================
class AudioService {
  final AudioRecorder _recorder = AudioRecorder();
  final AudioPlayer _player = AudioPlayer();

  StreamSubscription<Uint8List>? _recordingSubscription;
  bool _isRecording = false;

  static const int sampleRate = 16000;
  static const int numChannels = 1;
  static const int bitsPerSample = 16;

  /// 1280 bytes = 40ms audio @ 16kHz 16-bit mono (aman di bawah MTU WiFi 1472B)
  static const int udpChunkSize = 1280;

  final BytesBuilder _playbackBuffer = BytesBuilder(copy: false);
  bool _isPlayingChunk = false;

  AudioService() {
    _player.setReleaseMode(ReleaseMode.stop);
    _player.setPlayerMode(PlayerMode.lowLatency);
  }

  /// Mulai menangkap suara dari microphone (Raw PCM 16-bit Mono 16000Hz) dan
  /// mengalirkan potongan byte secara terus-menerus ke callback [onAudioChunk].
  Future<void> startRecording(void Function(Uint8List chunk) onAudioChunk) async {
    if (_isRecording) return;

    final hasPerm = await _recorder.hasPermission();
    if (!hasPerm) {
      debugPrint('Izin microphone belum diberikan oleh pengguna.');
      return;
    }

    _isRecording = true;
    final List<int> accumulator = [];

    final Stream<Uint8List> micStream = await _recorder.startStream(
      const RecordConfig(
        encoder: AudioEncoder.pcm16bits,
        sampleRate: sampleRate,
        numChannels: numChannels,
        autoGain: true,
        echoCancel: true,
        noiseSuppress: true,
      ),
    );

    _recordingSubscription = micStream.listen(
      (Uint8List rawBytes) {
        if (!_isRecording) return;
        accumulator.addAll(rawBytes);

        while (accumulator.length >= udpChunkSize) {
          final chunk = Uint8List.fromList(accumulator.sublist(0, udpChunkSize));
          accumulator.removeRange(0, udpChunkSize);
          onAudioChunk(chunk);
        }
      },
      onError: (Object err) {
        debugPrint('Error pada stream microphone: $err');
      },
    );
  }

  /// Menghentikan perekaman suara saat tombol PTT dilepas (`onLongPressEnd`).
  Future<void> stopRecording() async {
    _isRecording = false;
    await _recordingSubscription?.cancel();
    _recordingSubscription = null;

    if (await _recorder.isRecording()) {
      await _recorder.stop();
    }
  }

  /// Memutar data PCM mentah yang diterima dari jaringan UDP menggunakan
  /// BytesSource + Header WAV 44-byte di memori RAM (tanpa file perantara).
  Future<void> playAudioChunk(Uint8List pcmBytes) async {
    if (pcmBytes.isEmpty || _isRecording) return;

    _playbackBuffer.add(pcmBytes);

    // Kumpulkan minimal 3 paket (~120ms) agar pemutaran bebas patah-patah (jitter)
    if (_playbackBuffer.length < udpChunkSize * 3 || _isPlayingChunk) {
      return;
    }

    _isPlayingChunk = true;
    try {
      final rawPcm = _playbackBuffer.takeBytes();
      final wavBytes = _addWavHeader(rawPcm);
      await _player.play(BytesSource(wavBytes));
    } catch (e) {
      debugPrint('Gagal memutar chunk audio UDP: $e');
    } finally {
      _isPlayingChunk = false;
    }
  }

  /// Membuat header WAV 44-byte di memori untuk membungkus Raw PCM 16-bit 16kHz Mono
  Uint8List _addWavHeader(Uint8List pcmData) {
    final int dataLength = pcmData.length;
    final int fileSize = dataLength + 36;
    final int byteRate = sampleRate * numChannels * (bitsPerSample ~/ 8);
    final int blockAlign = numChannels * (bitsPerSample ~/ 8);

    final header = ByteData(44);
    header.setUint8(0, 0x52); // 'R'
    header.setUint8(1, 0x49); // 'I'
    header.setUint8(2, 0x46); // 'F'
    header.setUint8(3, 0x46); // 'F'
    header.setUint32(4, fileSize, Endian.little);
    header.setUint8(8, 0x57); // 'W'
    header.setUint8(9, 0x41); // 'A'
    header.setUint8(10, 0x56); // 'V'
    header.setUint8(11, 0x45); // 'E'
    header.setUint8(12, 0x66); // 'f'
    header.setUint8(13, 0x6D); // 'm'
    header.setUint8(14, 0x74); // 't'
    header.setUint8(15, 0x20); // ' '
    header.setUint32(16, 16, Endian.little);
    header.setUint16(20, 1, Endian.little); // 1 = PCM
    header.setUint16(22, numChannels, Endian.little);
    header.setUint32(24, sampleRate, Endian.little);
    header.setUint32(28, byteRate, Endian.little);
    header.setUint16(32, blockAlign, Endian.little);
    header.setUint16(34, bitsPerSample, Endian.little);
    header.setUint8(36, 0x64); // 'd'
    header.setUint8(37, 0x61); // 'a'
    header.setUint8(38, 0x74); // 't'
    header.setUint8(39, 0x61); // 'a'
    header.setUint32(40, dataLength, Endian.little);

    final wavBuffer = Uint8List(44 + dataLength);
    wavBuffer.setRange(0, 44, header.buffer.asUint8List());
    wavBuffer.setRange(44, 44 + dataLength, pcmData);
    return wavBuffer;
  }

  Future<void> dispose() async {
    await stopRecording();
    await _recorder.dispose();
    await _player.dispose();
  }
}

/// ============================================================================
/// 5. ROOT WIDGET & UI TAKTIKAL "DISKOMLEKAU TACTICAL PTT"
/// ============================================================================
class DiskomlekauTacticalPttApp extends StatelessWidget {
  const DiskomlekauTacticalPttApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'DISKOMLEKAU TACTICAL PTT',
      debugShowCheckedModeBanner: false,
      theme: ThemeData.dark().copyWith(
        scaffoldBackgroundColor: const Color(0xFF041D34),
        colorScheme: const ColorScheme.dark(
          primary: Color(0xFF23DCFF),
          secondary: Color(0xFF09B6DF),
        ),
      ),
      home: const TacticalPttHomePage(),
    );
  }
}

class TacticalPttHomePage extends StatefulWidget {
  const TacticalPttHomePage({super.key});

  @override
  State<TacticalPttHomePage> createState() => _TacticalPttHomePageState();
}

class _TacticalPttHomePageState extends State<TacticalPttHomePage> {
  int _selectedNavIndex = 0;

  static const Color bgDarkNavy = Color(0xFF041D34);
  static const Color cyanBright = Color(0xFF23DCFF);
  static const Color cyanSoft = Color(0xFF7CE8FF);

  /// Daftar anggota contoh saat diuji pada 1 perangkat; otomatis digantikan
  /// oleh daftar `activeChannelPeers` begitu ada perangkat lain terhubung ke Hotspot.
  final List<PeerMember> _fallbackMembers = [
    PeerMember(id: 'M1', callsign: 'YUDHI', ipAddress: '192.168.43.1', channel: 12),
    PeerMember(id: 'M2', callsign: 'EKO', ipAddress: '192.168.43.102', channel: 12),
    PeerMember(id: 'M3', callsign: 'PUJI', ipAddress: '192.168.43.103', channel: 12),
    PeerMember(id: 'M4', callsign: 'JEFRY', ipAddress: '192.168.43.104', channel: 12),
    PeerMember(id: 'M5', callsign: 'YUSUF', ipAddress: '192.168.43.105', channel: 12),
    PeerMember(id: 'M6', callsign: 'EDWIN', ipAddress: '192.168.43.106', channel: 12),
  ];

  void _showEditCallsignDialog(BuildContext context, UdpNetworkService net, {PeerMember? member}) {
    final controller = TextEditingController(
      text: member != null ? member.callsign : net.myCallsign,
    );
    showDialog(
      context: context,
      builder: (ctx) => AlertDialog(
        backgroundColor: const Color(0xFF05233E),
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(16),
          side: const BorderSide(color: cyanBright, width: 1.5),
        ),
        title: Text(
          member != null ? 'UBAH CALLSIGN ANGGOTA' : 'EDIT CALLSIGN SAYA',
          style: const TextStyle(color: cyanBright, fontWeight: FontWeight.bold, fontSize: 15),
        ),
        content: TextField(
          controller: controller,
          autofocus: true,
          textCapitalization: TextCapitalization.characters,
          style: const TextStyle(color: Colors.white, fontWeight: FontWeight.bold),
          decoration: InputDecoration(
            hintText: 'Masukkan Callsign',
            hintStyle: TextStyle(color: Colors.white.withValues(alpha: 0.4)),
            enabledBorder: OutlineInputBorder(
              borderSide: BorderSide(color: cyanBright.withValues(alpha: 0.5)),
            ),
            focusedBorder: const OutlineInputBorder(
              borderSide: BorderSide(color: cyanBright, width: 2),
            ),
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx),
            child: const Text('BATAL', style: TextStyle(color: Colors.white70)),
          ),
          ElevatedButton(
            style: ElevatedButton.styleFrom(backgroundColor: cyanBright),
            onPressed: () {
              final value = controller.text.trim().toUpperCase();
              if (value.isNotEmpty) {
                if (member != null) {
                  setState(() => member.callsign = value);
                } else {
                  net.setCallsign(value);
                  setState(() => _fallbackMembers[0].callsign = value);
                }
              }
              Navigator.pop(ctx);
            },
            child: const Text(
              'SIMPAN',
              style: TextStyle(color: Color(0xFF021627), fontWeight: FontWeight.bold),
            ),
          ),
        ],
      ),
    );
  }

  void _showBarcodeModal(BuildContext context, UdpNetworkService net) {
    showDialog(
      context: context,
      builder: (ctx) => AlertDialog(
        backgroundColor: const Color(0xFF05233E),
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(16),
          side: const BorderSide(color: cyanBright, width: 1.5),
        ),
        title: const Text(
          'BARCODE PAIRING HOTSPOT',
          textAlign: TextAlign.center,
          style: TextStyle(color: cyanBright, fontWeight: FontWeight.bold, fontSize: 15),
        ),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Container(
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: Colors.white,
                borderRadius: BorderRadius.circular(12),
              ),
              child: const Icon(Icons.qr_code_2, size: 160, color: Color(0xFF03192E)),
            ),
            const SizedBox(height: 12),
            Text(
              'HOST: ${net.localIp} • CH ${net.currentChannel}',
              style: const TextStyle(color: Colors.white, fontWeight: FontWeight.bold),
            ),
            const SizedBox(height: 4),
            Text(
              'UDP Broadcast Discovery Port: ${UdpNetworkService.discoveryPort}',
              style: const TextStyle(color: cyanSoft, fontSize: 11),
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx),
            child: const Text('TUTUP', style: TextStyle(color: cyanBright, fontWeight: FontWeight.bold)),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    // Consumer<UdpNetworkService> memastikan UI otomatis me-refresh seketika
    // saat perangkat baru terhubung ke WiFi Hotspot dan terdeteksi via UDP.
    return Consumer<UdpNetworkService>(
      builder: (context, net, _) {
        final audioService = context.read<AudioService>();
        final List<PeerMember> peers = net.activeChannelPeers.isNotEmpty
            ? net.activeChannelPeers
            : _fallbackMembers;
        final int activeCount = peers.length;

        return Scaffold(
          backgroundColor: bgDarkNavy,
          body: CustomPaint(
            painter: _TacticalGridPainter(),
            child: SafeArea(
              child: Column(
                children: [
                  Expanded(
                    child: SingleChildScrollView(
                      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
                      child: Column(
                        children: [
                          // 1. TEKS JUDUL "DISKOMLEKAU TACTICAL PTT"
                          const Padding(
                            padding: EdgeInsets.symmetric(vertical: 8),
                            child: Text(
                              'DISKOMLEKAU TACTICAL PTT',
                              textAlign: TextAlign.center,
                              style: TextStyle(
                                color: Color(0xFFE5FAFF),
                                fontSize: 24,
                                fontWeight: FontWeight.w900,
                                letterSpacing: 1.5,
                                shadows: [
                                  Shadow(color: cyanBright, blurRadius: 14),
                                ],
                              ),
                            ),
                          ),

                          // 2. CONTAINER CARD STATUS JARINGAN
                          _buildNetworkStatusCard(net, activeCount),
                          const SizedBox(height: 12),

                          // 3. CONTAINER CHANNEL SELECTOR
                          _buildChannelCard(context, net, activeCount),
                          const SizedBox(height: 14),

                          // 4. GRIDVIEW ANGGOTA (activeChannelPeers)
                          _buildMemberGridSection(context, net, peers),
                          const SizedBox(height: 18),

                          // 5. TOMBOL PTT RAKSASA DI TENGAH BAWAH
                          _buildGiantPttButton(net, audioService),
                          const SizedBox(height: 16),

                          // Banner Status Bawah
                          _buildBottomStatusBanner(net),
                        ],
                      ),
                    ),
                  ),

                  // 6. BOTTOM NAVIGATION BAR (4 IKON)
                  _buildBottomNavigationBar(context, net),
                ],
              ),
            ),
          ),
        );
      },
    );
  }

  /// 2. Container Card Status Jaringan: Menampilkan `localIp`, `isMaster` (Peran),
  ///    `lastPingMs`, dan jumlah anggota aktif dari provider.
  Widget _buildNetworkStatusCard(UdpNetworkService net, int activeCount) {
    return Container(
      padding: const EdgeInsets.all(14),
      decoration: _tacticalCardDecoration(),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 6),
                decoration: BoxDecoration(
                  color: const Color(0xFF03192D),
                  borderRadius: BorderRadius.circular(8),
                  border: Border.all(color: cyanBright.withValues(alpha: 0.45)),
                ),
                child: Row(
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    _buildSignalBar(8, true),
                    const SizedBox(width: 3),
                    _buildSignalBar(12, true),
                    const SizedBox(width: 3),
                    _buildSignalBar(16, true),
                    const SizedBox(width: 3),
                    _buildSignalBar(20, !net.isHighLatency),
                  ],
                ),
              ),
              const SizedBox(width: 10),
              Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  const Text(
                    'PING:',
                    style: TextStyle(color: Colors.white, fontWeight: FontWeight.bold, fontSize: 12),
                  ),
                  Text(
                    '${net.lastPingMs} ms',
                    style: const TextStyle(color: Colors.white, fontWeight: FontWeight.w900, fontSize: 15),
                  ),
                ],
              ),
              const SizedBox(width: 6),
              Text(
                '(+${net.pingDeltaMs.abs()}ms)',
                style: const TextStyle(color: cyanSoft, fontSize: 11.5),
              ),
              const SizedBox(width: 8),
              Expanded(
                child: Container(
                  padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                  decoration: BoxDecoration(
                    borderRadius: BorderRadius.circular(6),
                    border: Border.all(color: cyanBright.withValues(alpha: 0.5)),
                  ),
                  child: Text(
                    net.isHighLatency ? 'LATENCY\nTINGGI' : 'LATENCY\nRENDAH',
                    style: const TextStyle(
                      color: Colors.white,
                      fontWeight: FontWeight.bold,
                      fontSize: 10.5,
                      height: 1.15,
                    ),
                  ),
                ),
              ),
              const SizedBox(width: 6),
              InkWell(
                onTap: net.refreshPing,
                borderRadius: BorderRadius.circular(8),
                child: Container(
                  padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
                  decoration: BoxDecoration(
                    color: const Color(0xFF04223C),
                    borderRadius: BorderRadius.circular(8),
                    border: Border.all(color: cyanBright.withValues(alpha: 0.65)),
                  ),
                  child: const Row(
                    children: [
                      Icon(Icons.sync, size: 14, color: cyanBright),
                      SizedBox(width: 4),
                      Text(
                        'Ping',
                        style: TextStyle(color: Colors.white, fontWeight: FontWeight.bold, fontSize: 12),
                      ),
                    ],
                  ),
                ),
              ),
              const SizedBox(width: 6),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
                decoration: BoxDecoration(
                  color: cyanBright,
                  borderRadius: BorderRadius.circular(8),
                  boxShadow: [BoxShadow(color: cyanBright.withValues(alpha: 0.5), blurRadius: 10)],
                ),
                child: Row(
                  children: [
                    const Icon(Icons.star, size: 14, color: Color(0xFF022038)),
                    const SizedBox(width: 4),
                    Text(
                      net.isMaster ? 'MASTER' : 'CLIENT',
                      style: const TextStyle(
                        color: Color(0xFF022038),
                        fontWeight: FontWeight.w900,
                        fontSize: 12,
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ),
          const SizedBox(height: 10),
          Divider(color: cyanBright.withValues(alpha: 0.3), height: 1),
          const SizedBox(height: 10),
          Row(
            children: [
              Expanded(
                child: Text(
                  'PERAN: ${net.isMaster ? "HP MASTER" : "HP CLIENT"}\n(${net.localIp})',
                  style: const TextStyle(color: Colors.white, fontSize: 12, fontWeight: FontWeight.w600, height: 1.35),
                ),
              ),
              Expanded(
                child: Text(
                  'Peer Aktif: $activeCount • Target: ${net.targetedPeers.isEmpty ? activeCount : net.targetedPeers.length} activeClients\n(Unicast)',
                  style: const TextStyle(color: Colors.white, fontSize: 12, fontWeight: FontWeight.w600, height: 1.35),
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  /// 3. Container Channel: Tombol panah kiri/kanan untuk mengubah `currentChannel` di provider.
  Widget _buildChannelCard(BuildContext context, UdpNetworkService net, int activeCount) {
    return Container(
      padding: const EdgeInsets.all(14),
      decoration: _tacticalCardDecoration(),
      child: Column(
        children: [
          const Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Icon(Icons.arrow_drop_down, color: cyanSoft, size: 18),
              Text(
                'CHANNEL SELECTOR - CLOUD & HOTSPOT SYNC',
                style: TextStyle(
                  color: Colors.white,
                  fontWeight: FontWeight.w800,
                  fontSize: 12,
                  letterSpacing: 0.6,
                ),
              ),
              Icon(Icons.arrow_drop_down, color: cyanSoft, size: 18),
            ],
          ),
          const SizedBox(height: 10),
          Row(
            children: [
              InkWell(
                onTap: () => net.setChannel(net.currentChannel - 1),
                borderRadius: BorderRadius.circular(30),
                child: Container(
                  width: 52,
                  height: 52,
                  decoration: BoxDecoration(
                    shape: BoxShape.circle,
                    gradient: const RadialGradient(
                      colors: [Color(0xFF0D4C73), Color(0xFF042038)],
                    ),
                    border: Border.all(color: cyanBright, width: 2),
                    boxShadow: [BoxShadow(color: cyanBright.withValues(alpha: 0.25), blurRadius: 8)],
                  ),
                  child: const Icon(Icons.chevron_left, color: Colors.white, size: 28),
                ),
              ),
              const SizedBox(width: 12),
              Expanded(
                child: Container(
                  padding: const EdgeInsets.symmetric(vertical: 12, horizontal: 10),
                  decoration: BoxDecoration(
                    color: const Color(0xFF021629),
                    borderRadius: BorderRadius.circular(14),
                    border: Border.all(color: cyanBright.withValues(alpha: 0.45)),
                  ),
                  child: Column(
                    children: [
                      Text(
                        'CHANNEL ${net.currentChannel.toString().padLeft(2, '0')}',
                        style: const TextStyle(
                          color: Colors.white,
                          fontSize: 28,
                          fontWeight: FontWeight.w900,
                          letterSpacing: 2.0,
                        ),
                      ),
                      const SizedBox(height: 4),
                      Text(
                        'MASTER HOST - ${net.localIp}',
                        style: const TextStyle(
                          color: cyanSoft,
                          fontSize: 12,
                          fontWeight: FontWeight.w600,
                          letterSpacing: 0.8,
                        ),
                      ),
                    ],
                  ),
                ),
              ),
              const SizedBox(width: 12),
              InkWell(
                onTap: () => net.setChannel(net.currentChannel + 1),
                borderRadius: BorderRadius.circular(30),
                child: Container(
                  width: 52,
                  height: 52,
                  decoration: BoxDecoration(
                    shape: BoxShape.circle,
                    gradient: const RadialGradient(
                      colors: [Color(0xFF0D4C73), Color(0xFF042038)],
                    ),
                    border: Border.all(color: cyanBright, width: 2),
                    boxShadow: [BoxShadow(color: cyanBright.withValues(alpha: 0.25), blurRadius: 8)],
                  ),
                  child: const Icon(Icons.chevron_right, color: Colors.white, size: 28),
                ),
              ),
            ],
          ),
          const SizedBox(height: 12),
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text(
                'HP MASTER AKTIF | $activeCount MEMBERS ONLINE',
                style: const TextStyle(
                  color: Colors.white,
                  fontWeight: FontWeight.w800,
                  fontSize: 12,
                ),
              ),
              InkWell(
                onTap: () => _showBarcodeModal(context, net),
                borderRadius: BorderRadius.circular(8),
                child: Container(
                  padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                  decoration: BoxDecoration(
                    color: const Color(0xFF04223C),
                    borderRadius: BorderRadius.circular(8),
                    border: Border.all(color: cyanBright.withValues(alpha: 0.65)),
                  ),
                  child: const Row(
                    children: [
                      Icon(Icons.qr_code_2, size: 16, color: cyanBright),
                      SizedBox(width: 5),
                      Text(
                        'BARCODE',
                        style: TextStyle(color: Colors.white, fontWeight: FontWeight.w800, fontSize: 12),
                      ),
                    ],
                  ),
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  /// 4. GridView Anggota: Menampilkan daftar `activeChannelPeers`.
  Widget _buildMemberGridSection(
    BuildContext context,
    UdpNetworkService net,
    List<PeerMember> peers,
  ) {
    return Column(
      children: [
        Row(
          children: [
            InkWell(
              onTap: net.toggleTalkAll,
              borderRadius: BorderRadius.circular(24),
              child: Container(
                padding: const EdgeInsets.symmetric(horizontal: 18, vertical: 12),
                decoration: BoxDecoration(
                  color: net.talkAll ? cyanBright : const Color(0xFF062E4F),
                  borderRadius: BorderRadius.circular(24),
                  border: Border.all(color: cyanBright, width: 1.5),
                  boxShadow: [
                    if (net.talkAll)
                      BoxShadow(color: cyanBright.withValues(alpha: 0.55), blurRadius: 14),
                  ],
                ),
                child: Row(
                  children: [
                    Icon(
                      Icons.sensors,
                      size: 18,
                      color: net.talkAll ? const Color(0xFF021C33) : cyanBright,
                    ),
                    const SizedBox(width: 6),
                    Text(
                      'TALK ALL',
                      style: TextStyle(
                        color: net.talkAll ? const Color(0xFF021C33) : Colors.white,
                        fontWeight: FontWeight.w900,
                        fontSize: 13.5,
                      ),
                    ),
                  ],
                ),
              ),
            ),
            const SizedBox(width: 10),
            Expanded(
              child: Container(
                padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
                decoration: _tacticalCardDecoration(),
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    Flexible(
                      child: Text(
                        'ID SAYA: ${net.myCallsign}',
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(
                          color: Colors.white,
                          fontWeight: FontWeight.w900,
                          fontSize: 13,
                        ),
                      ),
                    ),
                    InkWell(
                      onTap: () => _showEditCallsignDialog(context, net),
                      child: Container(
                        padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 5),
                        decoration: BoxDecoration(
                          borderRadius: BorderRadius.circular(8),
                          border: Border.all(color: cyanBright.withValues(alpha: 0.6)),
                        ),
                        child: const Row(
                          children: [
                            Icon(Icons.edit, size: 13, color: cyanBright),
                            SizedBox(width: 4),
                            Text('Edit Nama', style: TextStyle(color: Colors.white, fontSize: 11.5)),
                          ],
                        ),
                      ),
                    ),
                  ],
                ),
              ),
            ),
          ],
        ),
        const SizedBox(height: 10),
        GridView.builder(
          shrinkWrap: true,
          physics: const NeverScrollableScrollPhysics(),
          itemCount: peers.length,
          gridDelegate: const SliverGridDelegateWithFixedCrossAxisCount(
            crossAxisCount: 3,
            mainAxisSpacing: 8,
            crossAxisSpacing: 8,
            childAspectRatio: 2.15,
          ),
          itemBuilder: (context, index) {
            final peer = peers[index];
            return InkWell(
              onTap: () {
                net.togglePeerTarget(peer.ipAddress);
                setState(() => peer.isTargeted = !peer.isTargeted);
              },
              borderRadius: BorderRadius.circular(14),
              child: Container(
                padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 6),
                decoration: BoxDecoration(
                  gradient: LinearGradient(
                    colors: peer.isTargeted
                        ? [const Color(0xFF1499C7), const Color(0xFF0A6691)]
                        : [const Color(0xFF062C4D), const Color(0xFF041D35)],
                    begin: Alignment.topCenter,
                    end: Alignment.bottomCenter,
                  ),
                  borderRadius: BorderRadius.circular(14),
                  border: Border.all(
                    color: peer.isTargeted ? cyanBright : cyanBright.withValues(alpha: 0.35),
                    width: peer.isTargeted ? 1.5 : 1,
                  ),
                ),
                child: Row(
                  children: [
                    const Icon(Icons.mic_none, size: 18, color: cyanBright),
                    const SizedBox(width: 6),
                    Expanded(
                      child: Column(
                        mainAxisAlignment: MainAxisAlignment.center,
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            peer.callsign,
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                            style: const TextStyle(
                              color: Colors.white,
                              fontWeight: FontWeight.w900,
                              fontSize: 13,
                            ),
                          ),
                          const Text(
                            'ALL AKTIF',
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                            style: TextStyle(
                              color: cyanSoft,
                              fontSize: 10,
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                        ],
                      ),
                    ),
                    InkWell(
                      onTap: () => _showEditCallsignDialog(context, net, member: peer),
                      child: const Icon(Icons.edit, size: 13, color: Colors.white),
                    ),
                  ],
                ),
              ),
            );
          },
        ),
      ],
    );
  }

  /// 5. Tombol PTT Raksasa di tengah bawah menggunakan GestureDetector:
  ///    - onLongPress: Ubah state `isTransmitting` menjadi true, jalankan
  ///      `AudioService.startRecording` dan teruskan byte-nya ke `UdpNetworkService.sendRawAudioChunk`.
  ///    - onLongPressEnd: Hentikan rekaman dan ubah state `isTransmitting` ke false.
  Widget _buildGiantPttButton(UdpNetworkService net, AudioService audioService) {
    final bool isTx = net.isTransmitting;

    return GestureDetector(
      onLongPress: () async {
        net.setTransmitting(true);
        await audioService.startRecording((Uint8List pcmChunk) {
          net.sendRawAudioChunk(pcmChunk);
        });
      },
      onLongPressEnd: (_) async {
        await audioService.stopRecording();
        net.setTransmitting(false);
      },
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 150),
        width: 215,
        height: 215,
        decoration: BoxDecoration(
          shape: BoxShape.circle,
          gradient: RadialGradient(
            colors: isTx
                ? [cyanBright, const Color(0xFF087CA6), const Color(0xFF042D4A)]
                : [const Color(0xFF1476A6), const Color(0xFF073A5E), const Color(0xFF031F36)],
          ),
          border: Border.all(color: cyanBright, width: isTx ? 4 : 2.5),
          boxShadow: [
            BoxShadow(
              color: cyanBright.withValues(alpha: isTx ? 0.75 : 0.35),
              blurRadius: isTx ? 42 : 24,
              spreadRadius: isTx ? 6 : 1,
            ),
          ],
        ),
        child: Stack(
          alignment: Alignment.center,
          children: [
            const SizedBox(
              width: 190,
              height: 190,
              child: CustomPaint(
                painter: _CurvedTextPainter(text: 'PUSH TO TALK & HOLD'),
              ),
            ),
            Container(
              width: 142,
              height: 142,
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                gradient: const RadialGradient(
                  colors: [Color(0xFF1A8EC4), Color(0xFF073B5E)],
                ),
                border: Border.all(color: cyanSoft.withValues(alpha: 0.65), width: 1.5),
              ),
              child: Column(
                mainAxisAlignment: MainAxisAlignment.center,
                children: [
                  Icon(
                    Icons.sensors,
                    size: 44,
                    color: isTx ? Colors.white : const Color(0xFF03233B),
                  ),
                  const SizedBox(height: 6),
                  Text(
                    isTx ? 'TRANSMITTING' : 'MASTER READY',
                    style: const TextStyle(
                      color: Colors.white,
                      fontWeight: FontWeight.w900,
                      fontSize: 13.5,
                      letterSpacing: 0.8,
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildBottomStatusBanner(UdpNetworkService net) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.symmetric(vertical: 14),
      decoration: BoxDecoration(
        gradient: const LinearGradient(
          colors: [Color(0xFF28E0FF), Color(0xFF09B6DF)],
        ),
        borderRadius: BorderRadius.circular(14),
        boxShadow: [
          BoxShadow(color: cyanBright.withValues(alpha: 0.45), blurRadius: 16),
        ],
      ),
      child: Text(
        net.isTransmitting ? 'TRANSMITTING RAW PCM UDP...' : 'MASTER READY',
        textAlign: TextAlign.center,
        style: const TextStyle(
          color: Colors.white,
          fontSize: 16,
          fontWeight: FontWeight.w900,
          letterSpacing: 1.2,
        ),
      ),
    );
  }

  /// 6. Bottom Navigation Bar dengan 4 ikon: Channels, Edit Nama, Barcode, Settings.
  Widget _buildBottomNavigationBar(BuildContext context, UdpNetworkService net) {
    final items = [
      (Icons.sensors, '[Channels]'),
      (Icons.edit_outlined, '[Edit Nama]'),
      (Icons.qr_code_2, '[Barcode]'),
      (Icons.settings_outlined, '[Settings]'),
    ];
    return Container(
      padding: const EdgeInsets.symmetric(vertical: 10),
      decoration: BoxDecoration(
        color: const Color(0xFF031729),
        border: Border(top: BorderSide(color: cyanBright.withValues(alpha: 0.35))),
      ),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceAround,
        children: List.generate(items.length, (i) {
          final isSelected = _selectedNavIndex == i;
          return InkWell(
            onTap: () {
              setState(() => _selectedNavIndex = i);
              if (i == 1) _showEditCallsignDialog(context, net);
              if (i == 2) _showBarcodeModal(context, net);
            },
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                Icon(items[i].$1, color: isSelected ? cyanBright : Colors.white, size: 22),
                const SizedBox(height: 4),
                Text(
                  items[i].$2,
                  style: TextStyle(
                    color: isSelected ? cyanBright : Colors.white,
                    fontWeight: FontWeight.w700,
                    fontSize: 12,
                  ),
                ),
              ],
            ),
          );
        }),
      ),
    );
  }

  Widget _buildSignalBar(double height, bool active) {
    return Container(
      width: 4,
      height: height,
      decoration: BoxDecoration(
        color: active ? cyanBright : Colors.white24,
        borderRadius: BorderRadius.circular(2),
      ),
    );
  }

  BoxDecoration _tacticalCardDecoration() {
    return BoxDecoration(
      gradient: const LinearGradient(
        colors: [Color(0xFF072D4F), Color(0xFF041C33)],
        begin: Alignment.topCenter,
        end: Alignment.bottomCenter,
      ),
      borderRadius: BorderRadius.circular(16),
      border: Border.all(color: cyanBright.withValues(alpha: 0.55), width: 1.4),
      boxShadow: [
        BoxShadow(color: cyanBright.withValues(alpha: 0.14), blurRadius: 14),
      ],
    );
  }
}

/// CustomPainter untuk teks melingkar "PUSH TO TALK & HOLD" pada tombol PTT
class _CurvedTextPainter extends CustomPainter {
  final String text;
  const _CurvedTextPainter({required this.text});

  @override
  void paint(Canvas canvas, Size size) {
    final radius = size.width / 2 - 14;
    final center = Offset(size.width / 2, size.height / 2);
    const totalArc = math.pi * 0.68;
    const startAngle = -math.pi / 2 - totalArc / 2;
    final charStep = totalArc / (text.length - 1);

    for (int i = 0; i < text.length; i++) {
      final angle = startAngle + i * charStep;
      final charOffset = Offset(
        center.dx + radius * math.cos(angle),
        center.dy + radius * math.sin(angle),
      );

      canvas.save();
      canvas.translate(charOffset.dx, charOffset.dy);
      canvas.rotate(angle + math.pi / 2);

      final tp = TextPainter(
        text: TextSpan(
          text: text[i],
          style: const TextStyle(
            color: Color(0xFF03233B),
            fontSize: 11.5,
            fontWeight: FontWeight.w900,
          ),
        ),
        textDirection: TextDirection.ltr,
      )..layout();

      tp.paint(canvas, Offset(-tp.width / 2, -tp.height / 2));
      canvas.restore();
    }
  }

  @override
  bool shouldRepaint(covariant CustomPainter oldDelegate) => false;
}

/// CustomPainter untuk motif grid transparan pada background navy blue
class _TacticalGridPainter extends CustomPainter {
  @override
  void paint(Canvas canvas, Size size) {
    final paint = Paint()
      ..color = const Color(0xFF23DCFF).withValues(alpha: 0.06)
      ..strokeWidth = 1;

    const step = 28.0;
    for (double x = 0; x < size.width; x += step) {
      canvas.drawLine(Offset(x, 0), Offset(x, size.height), paint);
    }
    for (double y = 0; y < size.height; y += step) {
      canvas.drawLine(Offset(0, y), Offset(size.width, y), paint);
    }
  }

  @override
  bool shouldRepaint(covariant CustomPainter oldDelegate) => false;
}
