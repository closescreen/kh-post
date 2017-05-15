import std.stdio, std.string, std.getopt, core.stdc.stdlib, std.regex, std.algorithm, std.concurrency, requests;
import std.process;

void main(string[] args) {
  auto server = environment.get("kh_server", "");
  string query;
  auto synopsis = "Send input data via HTTP POST to ClickHouse server.\nUsage: echo abc | kh-post options...";
  auto proto = "http://";
  auto port = "8123";
  auto chunk_size = 1024*1024;
  auto content_type = "application/binary";	
  bool deb = false;
  auto expect = [200];
  bool noinput = false;
  
  try{
    auto cli = getopt( args,
	"server|s", "server address f.e. kh1.myorg. May be used environment variable 'kh_server'.", &server,
	"query|q", "string for pass to server as first part of query", &query,
	"noinput|n", "stdin has no input data ( only --query=.. data to send )", &noinput,
	"content-type", "content-type header for input data [application/binary]", &content_type,
	"chunk-size", "chunk size in bites [1024*1024]", &chunk_size,
	"deb|d", "enable debug messages", &deb,
	"expect", "expect http codes: list of codes: --expect=404 # for good exit status", &expect,
	"port|p","server port [8123]", &port,
	"proto","server protocol [http://]", &proto,
    );
    if( cli.helpWanted ) defaultGetoptPrinter( synopsis, cli.options), exit(0);
  }
  catch(Throwable o) stderr.writeln(o.msg), exit(1);

  if( empty( server)) stderr.writeln("kh-post: server serveress not defined!"), exit(1);
  else if( !matchFirst( server, regex("https?://"))) server = proto ~ server; // 'kh.myorg' --> 'http://kh.myorg'
  !matchFirst( server, regex(`:\d+$`)) && ( server ~= ":" ~ port ); // 'http://kh.myorg' --> 'http://kh.myorg:8123'
  
  deb && stderr.writefln( "server: %s\n expect_codes: %s", server, expect);

  // childPostSender( bool deb, string server, string content_type )
  
  auto fin = stdin;
  auto fout = stdout;
  auto ferr = stderr;
  
  deb && ferr.writeln("spawn");
  auto childTid = spawn( &childPostSender, deb, server, content_type );

  bool myblock(Tid tid){ deb && writeln("blocking"); return false; }  
  
  setMaxMailboxSize( childTid, 10, OnCrowding.block);
  
  // send --query=... as first part of POST data with "\n" at end:
  if ( !query.empty ){
    deb && "Sending query: %s ...".writefln( query);
    childTid.send( ( query.chomp ~ "\n" ).representation );
  }
  
  deb && ferr.writefln("wait for input data... from %s", fin);
  if (!noinput) foreach( chunk; fin.byChunk( chunk_size) ){
        childTid.send( chunk.idup);
  }
  childTid.send(true);// that's all
  
  auto answer_code = receiveOnly!int();
  
  int my_exit_status = 0;
  if( !expect.canFind( answer_code)) my_exit_status = 1; 
  deb && ferr.writefln("Answer code: %s", answer_code);
  exit( my_exit_status);
}



void childPostSender( bool deb, string server, string content_type ){
  
  auto chunks = new Generator!( immutable(ubyte)[] )({
    for( bool run=true; run;){
        receive(
            ( immutable(ubyte)[] chunk){ 
                deb && stderr.writefln("Generator yeld chunk:\n%s", chunk);
                yield( chunk); 
            }, // получили часть данных
            ( bool stop){ 
                deb && stderr.writefln("Received: %s", stop);
                run=false; 
            }, // данные закончились
            ( Variant v){ stderr.writefln("Unexpected value received from owner thread: %s", v); },
        );
    }
  });

  auto rq = Request();
  rq.useStreaming = true;
  deb && stderr.writeln("Start post sending...");
  auto rs = rq.post( server, chunks, content_type);
  auto stream = rs.receiveAsRange();
  while(!stream.empty) {
    deb && stderr.writefln("Received %d bytes, total received %d from document legth %d", stream.front.length, rq.contentReceived, rq.contentLength);
    stdout.write( cast(string)stream.front );
    stream.popFront;
  }
  ownerTid.send( rs.code);
}



