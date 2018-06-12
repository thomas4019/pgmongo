function getIsMasterReply() {
  return {
    'ismaster': true,
    'maxBsonObjectSize': 16777216,
    'maxMessageSizeBytes': 48000000,
    'maxWriteBatchSize': 1000,
    'localTime': new Date('2018-06-01T07:08:25.204Z'),
    'maxWireVersion': 5,
    'minWireVersion': 0,
    'readOnly': false,
    'ok': 1
  }
}

function getBuildInfo() {
  return {
    'version': '3.4.9',
    'modules': [],
    'allocator': 'system',
    'javascriptEngine': 'mozjs',
    'sysInfo': 'deprecated',
    'versionArray': [3, 4, 9, 0],
    'openssl': { 'running': 'OpenSSL 1.0.2n  7 Dec 2017', 'compiled': 'OpenSSL 1.0.2l  25 May 2017' },
    'buildEnvironment': {
      'distmod': '',
      'distarch': 'x86_64',
      'cc': '/usr/bin/clang: Apple LLVM version 8.1.0 (clang-802.0.42)',
      'ccflags': '-I/usr/local/opt/openssl/include -fno-omit-frame-pointer -fno-strict-aliasing -ggdb -pthread -Wall -Wsign-compare -Wno-unknown-pragmas -Winvalid-pch -O2 -Wno-unused-local-typedefs -Wno-unused-function -Wno-unused-private-field -Wno-deprecated-declarations -Wno-tautological-constant-out-of-range-compare -Wno-unused-const-variable -Wno-missing-braces -Wno-inconsistent-missing-override -Wno-potentially-evaluated-expression -fstack-protector-strong -Wno-null-conversion -mmacosx-version-min=10.12 -fno-builtin-memcmp',
      'cxx': '/usr/bin/clang++: Apple LLVM version 8.1.0 (clang-802.0.42)',
      'cxxflags': '-Woverloaded-virtual -Wpessimizing-move -Wredundant-move -Wno-undefined-var-template -std=c++11',
      'linkflags': '-L/usr/local/opt/openssl/lib -pthread -Wl,-bind_at_load -fstack-protector-strong -mmacosx-version-min=10.12',
      'target_arch': 'x86_64',
      'target_os': 'osx'
    },
    'bits': 64,
    'debug': false,
    'maxBsonObjectSize': 16777216,
    'storageEngines': ['devnull', 'ephemeralForTest', 'mmapv1', 'wiredTiger'],
    'ok': 1
  }
}

function getServerStatus() {
  return {
    'host' : 'Thomass-MBP.hsd1.ca.comcast.net',
    'version' : '3.4.9',
    'process' : 'mongod',
    'pid' : 70211,
    'uptime' : 1835,
    'uptimeMillis' : 1834724,
    'uptimeEstimate' : 1834,
    'localTime' : new Date('2018-06-05T07:44:43.503Z')
  }
}

function replSetGetStatus() {
  return {
    'ok': 0,
    'errmsg': 'not running with --replSet',
    'code': 76,
    'codeName': 'NoReplicationEnabled'
  }
}

exports = module.exports = {
  getIsMasterReply,
  getBuildInfo,
  getServerStatus,
  replSetGetStatus
}
