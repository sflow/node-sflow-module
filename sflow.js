// You are free to use this software in compliance with the Adaptive Public
// License (APL): http://host-sflow.sourceforge.net/license.html
//
// You can obtain additional rights if you are also in compliance with the sFlow
// License: http://www.inmon.com/technology/sflowlicense.txt
// The purpose of the sFlow license is to ensure consistent and interoperable
// implementations of the sFlow standard and correct use of the sFlow trademark
// and logo.
//
// This module exports http counters and sampled requests using the sFlow
// protocol (see http://www.sflow.org).
//
// Requires Host sFlow (http://host-sflow.sourceforge.net) to export server
// performance metrics and to create config file for this module.
//
// The following lines show how to enable sFlow monitoring:
//
// var http = require('http');
// require('./sflow.js').instrument(http);
// http.createServer(function(request, response) { ... 

var fs    = require('fs');
var dgram = require('dgram');

var configFile           = '/etc/hsflowd.auto';
var UNKNOWN_IP4          = [0,0,0,0];
var SFLOW_PORT           = 6343;
var PROTOCOL_TCP         = 6;
var DS_CLASS_LOGICAL     = 3;

var method_option_count  = 0;
var method_get_count     = 0;
var method_head_count    = 0;
var method_post_count    = 0;
var method_put_count     = 0;
var method_delete_count  = 0;
var method_trace_count   = 0;
var method_connect_count = 0;
var method_other_count   = 0;
var status_1XX_count     = 0;
var status_2XX_count     = 0;
var status_3XX_count     = 0;
var status_4XX_count     = 0;
var status_5XX_count     = 0;
var status_other_count   = 0;

var agent_ip             = UNKNOWN_IP4;
var sub_agent_index      = 0;
var ds_class             = DS_CLASS_LOGICAL;
var ds_index             = 0;
var sampling_threshold   = 0;
var sampling_rate        = 0;
var polling_interval     = 0;

var collector_sockets    = [];

var sample_pool          = 0;
var sample_count         = 0;

var agent_seq_no         = 0;
var counter_seq_no       = 0;
var flow_seq_no          = 0;

var agent_start_time     = 0;

// uniquely identify each server instance
var server_context       = [];

function createServerID() {
  var id = server_context.length;
  server_context.push({});
  return id;
}

function getServerContext(serverID) {
  return server_context[serverID];
}

var socket = dgram.createSocket('udp4');
function sendDatagram(message,length) {
  if(!socket || collector_sockets.length == 0) return;

  for(var i = 0; i < collector_sockets.length; i++) {
    var sock = collector_sockets[i];
    socket.send(message, 0, length, sock.port, sock.ip); 
  }
}

function ip4ToBytes(str) {
  if(!str) return UNKNOWN_IP4;

  var parts = str.split('.');
  if(parts.length != 4) return UNKNOWN_IP4;

  var bytes = new Array(4);
  for(var i = 0; i < 4; i++) bytes[i] = parseInt(parts[i]);

  return bytes;
}

function xdrInt(buf,offset,val) {
  var i = offset;
  buf[i++] = (val >>> 24) & 0xFF;
  buf[i++] = (val >>> 16) & 0xFF;
  buf[i++] = (val >>> 8) & 0xFF;
  buf[i++] = val & 0xFF;
  return i;
}

function xdrLong(buf,offset,val) {
  var i = offset;
  buf[i++] = (val >>> 56) & 0xFF;
  buf[i++] = (val >>> 48) & 0xFF;
  buf[i++] = (val >>> 40) & 0xFF;
  buf[i++] = (val >>> 32) & 0xFF;
  buf[i++] = (val >>> 24) & 0xFF;
  buf[i++] = (val >>> 16) & 0xFF;
  buf[i++] = (val >>> 8) & 0xFF;
  buf[i++] = val & 0xFF;
  return i;
}

function xdrIP4(buf,offset,ip4) {
  var i = offset;
  buf[i++] = ip4[0] & 0xFF;
  buf[i++] = ip4[1] & 0xFF;
  buf[i++] = ip4[2] & 0xFF;
  buf[i++] = ip4[3] & 0xFF;
  return i;
}

function xdrString(buf,offset,str,maxLen) {
  var i = offset;
  if(str) {
    var len = Math.min(str.length,maxLen);
    i = xdrInt(buf,i,len);
    for(var idx = 0; idx < len; idx++) {
      buf[i++] = str.charCodeAt(idx) & 0xFF;
    }
    var pad_bytes = (4 - len) & 3;
    if(pad_bytes) {
      for(var p = 0; p < pad_bytes; p++) buf[i++] = 0x00;
    }
  } else {
    i = xdrInt(buf,i,0);
  }
  return i;
}

function xdrDatasource(buf,offset,dsClass,dsIndex) {
  var i = offset;
  buf[i++] = dsClass & 0xFF;
  buf[i++] = (dsIndex >>> 16) & 0xFF;
  buf[i++] = (dsIndex >>> 8) & 0xFF;
  buf[i++] = dsIndex & 0xFF;
  return i;
}

var lastConfig = 0;
function readConfig(lastModifiedTime) {
 var lastModified = lastModifiedTime.valueOf();
 if(lastModified <= lastConfig) return;

 fs.readFile(configFile,"ascii", function(err,data) {
    if (err || !data) return;

    var rev_start = null;
    var rev_end = null;
    var sampling = null;
    var sampling_http = null;
    var polling = null;
    var agentIP = null;
    var collectors = [];

    data.split('\n').forEach(function(line) {
      if(line.charAt(0) == '#') return;
      var idx = line.indexOf('=');
      if(idx == -1) return;
      var key = line.substring(0,idx);
      var val = line.substring(idx + 1);

      switch(key) {
        case "rev_start"     : rev_start     = val; break;
        case "rev_end"       : rev_end       = val; break;
        case "sampling"      : sampling      = val; break;
        case "sampling.http" : sampling_http = val; break;
        case "polling"       : polling       = val; break;
        case "agentIP"       : agentIP       = val; break;
        case "collector"     : collectors.push(val); break;
      }

      if(!(rev_start && rev_end && rev_start == rev_end)) return;

      lastConfig = lastModified;

      if(polling) polling_interval = 1000 * parseInt(polling);
      else polling_interval = 0;

      if(!sampling_http) sampling_http = sampling;
      if(sampling_http) {
        sampling_rate = parseInt(sampling_http);
        if(sampling_rate > 0) sampling_threshold = 1 / sampling_rate;
        else sampling_threshold = 0;
      } else {
        sampling_rate      = 0;
        sampling_threshold = 0;
      }

      if(agentIP) agent_ip = ip4ToBytes(agentIP);
      else agent_ip = UNKNOWN_IP4;

      if(collectors.length == 0) collector_sockets = [];
      else {
        var sockets = [];
        for(var i = 0; i < collectors.length; i++) {
          var parts = collectors[i].split(' ');
          var ip = parts[0];
          var port = SFLOW_PORT;
          if(parts.length > 1) port = parts[1];
          sockets.push({ip: ip, port: port});
        }
        collector_sockets = sockets; 
      }
    });
  });
}

function checkConfig() {
  fs.stat(configFile, function(err,stats) {
    if (err || !stats) return;

    readConfig(stats.mtime); 
  });
}

var header_len = 24;
function writeHeader(buf,offset,now) {
  var i = offset;
  i = xdrInt(buf,i,5); // sFlow version 5
  i = xdrInt(buf,i,1); // IPv4 agent address
  i = xdrIP4(buf,i,agent_ip);
  i = xdrInt(buf,i,sub_agent_index);
  i = xdrInt(buf,i,agent_seq_no++);
  i = xdrInt(buf,i,now - agent_start_time);
  return i;
}

var counter_data_len = 88;
function writeCounters(buf,offset,now) {
  i = offset;
  i = xdrInt(buf,i,2);    // counter sample

  // make space for sample length
  var sample_len_idx = i;
  i += 4;

  i = xdrInt(buf,i,counter_seq_no++);
  i = xdrDatasource(buf,i,ds_class,ds_index);
  i = xdrInt(buf,i,1);    // 1 counter record
  i = xdrInt(buf,i,2201); // data format

  // make space for opaque length
  var opaque_len_idx = i;
  i += 4;

  i = xdrInt(buf,i,method_option_count);
  i = xdrInt(buf,i,method_get_count);
  i = xdrInt(buf,i,method_head_count);
  i = xdrInt(buf,i,method_post_count);
  i = xdrInt(buf,i,method_put_count);
  i = xdrInt(buf,i,method_delete_count);
  i = xdrInt(buf,i,method_trace_count);
  i = xdrInt(buf,i,method_connect_count);
  i = xdrInt(buf,i,method_other_count);
  i = xdrInt(buf,i,status_1XX_count);
  i = xdrInt(buf,i,status_2XX_count);
  i = xdrInt(buf,i,status_3XX_count);
  i = xdrInt(buf,i,status_4XX_count);
  i = xdrInt(buf,i,status_5XX_count);
  i = xdrInt(buf,i,status_other_count);

  // fill in the lengths
  xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
  xdrInt(buf,sample_len_idx, i - sample_len_idx - 4);

  return i;
}

function counterSample(now) {
  var buf = new Buffer(header_len + 4 + counter_data_len);
  var i = 0;
  i = writeHeader(buf,i,now);
  i = xdrInt(buf,i,1);    // 1 sample
  i = writeCounters(buf,i,now);

  sendDatagram(buf,i);
}

var max_flow_data_len = 1024;
function writeFlow(buf,offset,serverID,req,res,duration) {
  i = offset;
  i = xdrInt(buf,i,1);    // flow sample

  // make space for sample length
  var sample_len_idx = i;
  i += 4; 

  i = xdrInt(buf,i,flow_seq_no++);
  i = xdrDatasource(buf,i,ds_class,ds_index);
  i = xdrInt(buf,i,sampling_rate);
  i = xdrInt(buf,i,sample_pool);
  i = xdrInt(buf,i,0); // drops
  i = xdrInt(buf,i,0); // input interface
  i = xdrInt(buf,i,0x3FFFFFFF); // output interface
  i = xdrInt(buf,i, req.connection ? 2 : 1); // number of records
  i = xdrInt(buf,i,2201); // data format

  // make space for opaque length
  var opaque_len_idx = i;
  i += 4;

  // method
  var method_val = 0;
  switch(req.method) {
    case "OPTION" : method_val = 1; break;
    case "GET"    : method_val = 2; break;
    case "HEAD"   : method_val = 3; break;
    case "POST"   : method_val = 4; break;
    case "PUT"    : method_val = 5; break;
    case "DELETE" : method_val = 6; break;
    case "TRACE"  : method_val = 7; break;
    case "CONNECT": method_val = 8; break;
    default: method_val = 0;
  }
  i = xdrInt(buf,i,method_val);

  // protocol
  var protocol_val = req.httpVersionMajor * 1000 + req.httpVersionMinor;
  i = xdrInt(buf,i,protocol_val); 

  i = xdrString(buf,i,req.url,255);

  var host = req.headers['host'];
  i = xdrString(buf,i,host,32);

  // referer
  var referer = req.headers['referer'];
  i = xdrString(buf,i,referer,255);

  var useragent = req.headers['user-agent'];
  i = xdrString(buf,i,useragent,64);

  var authuser = null;
  i = xdrString(buf,i,authuser,32);

  var mimeType = null;
  i = xdrString(buf,i,mimeType,32);

  var resp_bytes = res.contentLength && res.contentLength > 0 ? res.contentLength : 0;
  i = xdrLong(buf,i,resp_bytes);

  i = xdrInt(buf,i,duration);
  i = xdrInt(buf,i,res.statusCode);

  // fill in length of http_request
  xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
 
  var connection = req.connection;
  if(connection) {
    i = xdrInt(buf,i,2100);  // ipv4 socket

    // make space for socket length
    // make space for opaque length
    var opaque_len_idx = i;
    i += 4;

    var context = getServerContext(serverID);

    var localAddr = connection.localAddress;
    if(!localAddr) localAddr = context.address;

    var localPort = connection.localPort;
    if(!localPort) localPort = context.port;

    i = xdrInt(buf,i,PROTOCOL_TCP);
    i = xdrIP4(buf,i,ip4ToBytes(localAddr));
    i = xdrIP4(buf,i,ip4ToBytes(connection.remoteAddress));
    i = xdrInt(buf,i,localPort);
    i = xdrInt(buf,i,connection.remotePort);

    // fill in length of extended_socket_ipv4
    xdrInt(buf,opaque_len_idx, i - opaque_len_idx - 4);
  }

  // fill in sample length
  xdrInt(buf,sample_len_idx, i - sample_len_idx - 4);

  return i;
}

function flowSample(serverID,req,res,duration) {
  var now = Date.now();

  var buf = new Buffer(header_len + 4 + max_flow_data_len);
  var i = 0;
  i = writeHeader(buf,i,now);
  i = xdrInt(buf,i,1);    // 1 sample
  i = writeFlow(buf,i,serverID,req,res,duration);

  sendDatagram(buf,i);
}

var lastPollCounters = 0;
function tick() {
  checkConfig();
  if(polling_interval > 0) {
    var now = Date.now();
    if(now - lastPollCounters > polling_interval) {
      counterSample(now);
      lastPollCounters = now;
    }
  }
}

function startAgent(serverID,port,address) {
  var context = getServerContext(serverID);
  context.port = port;
  context.address = address;

  if(sub_agent_index) return;

  sub_agent_index = port;
  ds_index = port;

  agent_start_time = Date.now();
  checkConfig();
  setInterval(tick, 1000);
}

function sample(serverID, req, res) {
  req._start_time = Date.now();
  var conn = req.connection;
  if(!conn.localAddress || !conn.localPort) {
    try {
      var soc = conn.address();
      if(soc) {
        if(!conn.localAddress) conn.localAddress = soc.address;
        if(!conn.localPort) conn.localPort = soc.port;
      }
    } catch(err) { ; }
  }
  var end = res.end;
  res.end = function () {
    switch(req.method) {
      case "OPTION" : method_option_count++;  break;
      case "GET"    : method_get_count++;     break;
      case "HEAD"   : method_head_count++;    break;
      case "POST"   : method_post_count++;    break;
      case "PUT"    : method_put_count++;     break;
      case "DELETE" : method_delete_count++;  break;
      case "TRACE"  : method_trace_count++;   break;
      case "CONNECT": method_connect_count++; break;
      default: method_other_count++; 
    }

    var status = res.statusCode;
    if(status < 100) status_other_count++;
    else if(status < 200) status_1XX_count++;
    else if(status < 300) status_2XX_count++;
    else if(status < 400) status_3XX_count++;
    else if(status < 500) status_4XX_count++;
    else if(status < 600) status_5XX_count++;
    else status_other_count++;

    sample_pool++; 
    if(sampling_rate
       && Math.random() < sampling_threshold) {
      sample_count++;
      var duration = req._start_time ? Date.now() - req._start_time : 0;
      flowSample(serverID,req,res,duration);
    }

    return end.apply(this, arguments);
  }
  var writeHead = res.writeHead;
  res.writeHead = function (code,headers) {
    res.statusCode = code;
    if(headers && typeof headers != 'string') {
       res.contentLength = headers['Content-Length'];
    }
    return writeHead.apply(this, arguments);
  }
  var write = res.write
}

var wrapHandler = function(serverID,fn) {
  return function() {
     sample(serverID,arguments[0],arguments[1]);
     return fn.apply(this,arguments);
  }
};

exports.instrument = function(http) {
  var createServer = http.createServer;
  http.createServer = function(handler) {
     var serverID = createServerID();
     arguments[0] = wrapHandler(serverID,handler);
     var server = createServer.apply(this,arguments);
     var listen = server.listen;
     server.listen = function(port,addr) {
       startAgent(serverID,port,addr);
       return listen.apply(this,arguments);
     }
     return server;
  }
}

