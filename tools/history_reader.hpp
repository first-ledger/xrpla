#pragma once
// Experimental XRPH v2/v3 reader. Canonical bytes and all recorded digests are
// checked before a result is reported; no transaction execution is performed.
#include <sodium.h>
#include <zstd.h>
#include <openssl/evp.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <sys/resource.h>

namespace xrpla::history {
using S=std::string; using V=std::string_view; using U=std::uint64_t;
using Wide=unsigned __int128; using Signed=__int128;
using Clock=std::chrono::steady_clock; using Json=nlohmann::json;
constexpr U recordMax=16ULL<<20, sectionMax=128ULL<<20, segmentMax=512ULL<<20, itemsMax=4000000;
inline bool opensslHashes=true;
inline void require(bool ok,char const* message){if(!ok)throw std::runtime_error(message);}
inline double elapsed(Clock::time_point start){return std::chrono::duration<double>(Clock::now()-start).count();}
// One OpenSSL context per thread and algorithm, fetched once: the one-shot SHA256()/SHA512()
// look the algorithm up and allocate a context on every call (about 2x slower on small inputs).
struct Digester{EVP_MD* md;EVP_MD_CTX* ctx;explicit Digester(char const* name):md(EVP_MD_fetch(nullptr,name,nullptr)),ctx(EVP_MD_CTX_new()){require(md&&ctx,"Digest unavailable");}~Digester(){EVP_MD_CTX_free(ctx);EVP_MD_free(md);}Digester(Digester const&)=delete;Digester& operator=(Digester const&)=delete;
 void run(V v,unsigned char* out){unsigned n=0;require(EVP_DigestInit_ex2(ctx,md,nullptr)==1&&EVP_DigestUpdate(ctx,v.data(),v.size())==1&&EVP_DigestFinal_ex(ctx,out,&n)==1,"Digest failed");}};
inline S sha(V v){S out(32,'\0');auto p=reinterpret_cast<unsigned char const*>(v.data());auto q=reinterpret_cast<unsigned char*>(out.data());if(opensslHashes){thread_local Digester d("SHA256");d.run(v,q);}else crypto_hash_sha256(q,p,v.size());return out;}
inline S half(V v){S out(64,'\0');auto p=reinterpret_cast<unsigned char const*>(v.data());auto q=reinterpret_cast<unsigned char*>(out.data());if(opensslHashes){thread_local Digester d("SHA512");d.run(v,q);}else crypto_hash_sha512(q,p,v.size());out.resize(32);return out;}
inline S hex(V v){static char const* digits="0123456789abcdef";S out;out.reserve(2*v.size());for(unsigned char c:v){out+=digits[c>>4];out+=digits[c&15];}return out;}
inline void le(S& out,U n,unsigned width){for(unsigned i=0;i<width;++i){out+=char(n&255);n>>=8;}}
inline void vu(S& out,U n){while(n>=128){out+=char((n&127)|128);n>>=7;}out+=char(n);}
inline void blob(S& out,V v){vu(out,v.size());out.append(v);}
inline void rec(S& out,V v){le(out,v.size(),4);out.append(v);}
struct Reader {
 V data; std::size_t pos=0;
 V take(U n){require(n<=data.size()-pos,"Truncated data");auto v=data.substr(pos,n);pos+=n;return v;}
 U byte(){return static_cast<unsigned char>(take(1)[0]);}
 U little(unsigned n){U v=0;for(unsigned i=0;i<n;++i)v|=byte()<<(8*i);return v;}
 Wide wide(Wide limit){Wide v=0;for(unsigned i=0;i<19;++i){U b=byte();require(i<18||(b&127)<=3,"Varint overflow");v|=Wide(b&127)<<(7*i);if(!(b&128)){require(v<=limit&&(!i||b),"Invalid varint");return v;}}throw std::runtime_error("Oversized varint");}
 // One- and two-byte varints (nearly all of a segment dictionary's) without wide()'s general
 // loop, under the same checks: canonical (no zero final byte after the first) and within limit.
 U var(U limit=segmentMax){
  if(pos<data.size()){U b0=static_cast<unsigned char>(data[pos]);
   if(!(b0&128)){require(b0<=limit,"Invalid varint");++pos;return b0;}
   if(pos+1<data.size()){U b1=static_cast<unsigned char>(data[pos+1]);
    if(!(b1&128)){U v=(b0&127)|(b1<<7);require(b1&&v<=limit,"Invalid varint");pos+=2;return v;}}}
  return static_cast<U>(wide(limit));}
 V bytes(){return take(var());}
 // Past `n` varints without decoding them (their values and form are checked where they are read): eight bytes at a
 // time, counting the bytes that end a varint (high bit clear). Little-endian loads, as on every supported platform.
 void skip(U n=1){while(n){if(pos+8<=data.size()){U w;std::memcpy(&w,data.data()+pos,8);U ends=~w&0x8080808080808080ULL;U c=static_cast<U>(std::popcount(ends));
   if(c>=n){for(U i=1;i<n;++i)ends&=ends-1;pos+=(static_cast<U>(std::countr_zero(ends))>>3)+1;return;}pos+=8;n-=c;continue;}
  require(pos<data.size(),"Truncated varint");if(!(static_cast<unsigned char>(data[pos++])&128))--n;}}
 void end(){require(pos==data.size(),"Trailing data");}
};
template<class T> T const& get(std::vector<T> const& v,U i){require(i<v.size(),"Reference out of range");return v[static_cast<std::size_t>(i)];}
inline S body(std::vector<std::pair<U,S>> const& rows){S out,table;U offset=0;for(auto const& [seq,row]:rows){le(table,seq,4);le(table,offset,8);out+=row;offset+=row.size();require(offset+table.size()<=segmentMax,"Canonical body bound");}out+=table;return out;}

// Transaction columns are completely independent of the history dictionary.
inline S transactionColumns(V data){
 Reader r{data};require(r.take(4)=="XTC1","Transaction columns magic");auto header=r.bytes();
 using Shape=std::vector<std::pair<V,U>>;std::vector<Shape> shapes;
 auto ns=r.var(std::min<U>(1000000,data.size()));shapes.reserve(ns);
 for(U i=0;i<ns;++i){Shape s;auto n=r.var(65536);for(U j=0;j<n;++j){auto lit=r.bytes();auto c=r.var(65536);s.emplace_back(lit,c);}shapes.push_back(std::move(s));}
 Reader layout{r.bytes()};std::vector<std::vector<S>> columns;U used=0;
 auto nc=r.var(65536);columns.reserve(nc);
 for(U ci=0;ci<nc;++ci){
  U width=r.var(64);Reader c{r.bytes()};U mode=c.byte(),count=c.var();Reader q{c.bytes()};c.end();
  require(count<=sectionMax&&(!width||count*width<=sectionMax-used),"Column expansion");std::vector<S> values;
  auto value=[&](){auto v=width?q.take(width):q.bytes();require(v.size()<=sectionMax,"Column value bound");return S(v);};
  U total=0;auto add=[&](S v){total+=v.size();require(total<=sectionMax-used,"Column budget");values.push_back(std::move(v));};
  if(mode==0){for(U i=0;i<count;++i)add(value());}
  else if(mode==1){U n=q.var();require(n<=count,"Column dictionary size");std::vector<S> dict;U bytes=0;for(U i=0;i<n;++i){auto v=value();bytes+=v.size();require(bytes<=sectionMax-used,"Column dictionary bound");dict.push_back(std::move(v));}for(U i=0;i<count;++i)add(get(dict,q.var()));}
  else if(mode==2){require(width>=1&&width<=8,"Numeric column width");Signed last=0;Wide bound=Wide(1)<<(width*8);for(U i=0;i<count;++i){auto n=q.wide(bound*2);Signed delta=(n&1)?-Signed((n>>1)+1):Signed(n>>1);last+=delta;require(last>=0&&Wide(last)<bound,"Numeric overflow");S v(width,'\0');U x=static_cast<U>(last);for(U j=0;j<width;++j){v[width-1-j]=char(x&255);x>>=8;}add(std::move(v));}}
  else throw std::runtime_error("Column mode");
  q.end();used+=total;columns.push_back(std::move(values));
 }
 r.end();Reader h{header};U n=h.var(256);S out;vu(out,n);std::vector<U> positions(columns.size());
 for(U i=0;i<n;++i){U seq=h.little(4);auto hdr=h.bytes();U count=h.var(1000000);le(out,seq,4);blob(out,hdr);vu(out,count);
  for(U j=0;j<count;++j){U size=layout.var(recordMax),sid=layout.var(shapes.size());S tx;
   if(!sid)tx=layout.bytes();else for(auto const& [literal,cid]:get(shapes,sid-1)){tx+=literal;if(cid){auto const& col=get(columns,cid-1);tx+=get(col,positions.at(cid-1)++);}require(tx.size()<=size,"Transaction expansion");}
   require(tx.size()==size,"Transaction size");blob(out,tx);require(out.size()<=sectionMax,"Transaction page bound");
  }
 }
 h.end();layout.end();for(U i=0;i<columns.size();++i)require(positions[i]==columns[i].size(),"Unconsumed column");return out;
}

class Archive {
 struct Page {U first,count,txFirst,txCount;S digest;};
 struct Section {U offset,length,size;S digest;};
 // Dictionary entries are small fixed records. A hash recipe's or atom's arguments stay in the
 // dictionary (`off` is where they start; a stored hash's `off` is the hash itself) and are read
 // when it is first rendered, then the value is memoized in memo_. Loading a segment's
 // dictionary is then one pass that records positions: decoding every argument into tables
 // (about two million per 2014 segment) was most of a cold read and most of its memory.
 // 8 bytes each: a 2025 segment's dictionary holds about two million of them (an open segment's largest
 // table). A hash's recipe mode is the byte before `off` (a stored hash's mode is 0 and `off` is the hash);
 // an atom's record starts at `off` with its header and kind, read again when it is rendered. A value is the
 // offset of its length-prefixed bytes in the dictionary.
 struct Identity {std::uint32_t off=0,memo=0;};
 struct Atom {std::uint32_t off=0,memo=0;};
 static_assert(sizeof(Identity)==8&&sizeof(Atom)==8,"Dictionary records are 8 bytes");
 static constexpr U recipeArgs[]={0,1,1,2,3,2,2,1,5,1,1};
 struct TxRow {U seq;V header;std::vector<V> txs;};
 struct TxPage {S raw;std::vector<TxRow> rows;std::vector<V> flat;bool ready=false;};
 V data_;U version_;S digest_,dictionary_;bool dictionaryReady_=false;std::vector<Page> pages_;std::vector<Section> sections_;
 // Sections whose checksum matched since this archive was opened: a section is decompressed again and again
 // (a cursor per page read, each transaction page), and hashing it each time was a fifth of a cold read.
 std::unique_ptr<std::atomic<bool>[]> sectionChecked_;
 std::vector<V> headers_;std::vector<std::uint32_t> values_;std::vector<Identity> hashes_;std::vector<Atom> atoms_;std::deque<S> memo_;
 // Each atom's header, apart from its record, for the shape checks of every view decoded: one byte where the
 // dictionary has at most 256 headers (about 80 in a 2025 segment), else two.
 std::vector<std::uint8_t> atomHeaders8_;std::vector<std::uint16_t> atomHeaders16_;
 V remember(std::uint32_t& memo,S value){require(memo_.size()<UINT32_MAX,"Memo size");heldBytes_+=sizeof(S)+value.capacity();memo_.push_back(std::move(value));memo=static_cast<std::uint32_t>(memo_.size());return memo_.back();}std::vector<std::vector<U>> shapes_;
 std::vector<TxPage> txPages_;std::size_t heldBytes_=0;using Cache=std::unordered_map<U,std::vector<U>>;
 S section(U i){auto const& s=get(sections_,i);auto frame=data_.substr(s.offset,s.length);if(!sectionChecked_[i].load(std::memory_order_acquire)){require(sha(frame)==s.digest,"Section checksum differs");sectionChecked_[i].store(true,std::memory_order_release);}auto n=ZSTD_getFrameContentSize(frame.data(),frame.size());require(n==s.size&&n<=sectionMax,"Zstd size differs");S raw(n,'\0');
  // One decompression context per thread: a one-shot decompress makes one for every section.
  thread_local std::unique_ptr<ZSTD_DCtx,decltype(&ZSTD_freeDCtx)> context(ZSTD_createDCtx(),ZSTD_freeDCtx);auto result=ZSTD_decompressDCtx(context.get(),raw.data(),raw.size(),frame.data(),frame.size());require(!ZSTD_isError(result)&&result==n,"Zstd decode failed");return raw;}
 // Seconds the dictionary took to read (checksum and zstd) and to parse, for a reader's statistics.
 // Seconds the dictionary took to check and decompress, and the whole load (the two overlap), for a reader's statistics.
 double dictionaryReadSeconds_=0,dictionaryParseSeconds_=0;
 // While the dictionary decompresses on a second thread, how much of it has arrived (`failedAt` when decompression
 // failed); the parse waits for the bytes it is about to read. Null when the dictionary is whole.
 static constexpr U failedAt=std::numeric_limits<U>::max();
 std::atomic<U>* arriving_=nullptr;U arrived_=std::numeric_limits<U>::max();
 void need(U end){if(end<=arrived_)return;end=std::min<U>(end,dictionary_.size());
  for(U v;(v=arriving_->load(std::memory_order_acquire))<end;){require(v!=failedAt,"Zstd decode failed");arriving_->wait(v,std::memory_order_acquire);}
  arrived_=arriving_->load(std::memory_order_acquire);}
 // Bytes with a varint length, once they have arrived.
 V arrivedBytes(Reader& r){need(r.pos+32);U n=r.var();need(r.pos+n);return r.take(n);}
 // The dictionary is decompressed on a second thread while this one parses what has arrived, and its checksum computed on
 // a third: on a 2025 segment (about 11 MB compressed, 28 MB whole) each takes tens of milliseconds. The parse is bounds
 // checked like any untrusted input, and the dictionary is used only once the checksum matched.
 void loadDictionary(){if(dictionaryReady_)return;auto t=Clock::now();
  auto const& s=get(sections_,0);auto frame=data_.substr(s.offset,s.length);
  bool checked=false;std::thread hasher([&]{checked=sha(frame)==s.digest;});
  struct JoinHasher{std::thread& h;~JoinHasher(){if(h.joinable())h.join();}} joinHasher{hasher};
  // A failure reports a checksum mismatch first: corrupt bytes are the likelier cause.
  auto fail=[&](char const* message){if(hasher.joinable())hasher.join();require(checked,"Section checksum differs");throw std::runtime_error(message);};
  auto n=ZSTD_getFrameContentSize(frame.data(),frame.size());if(!(n==s.size&&n<=sectionMax))fail("Zstd size differs");dictionary_.assign(n,'\0');
  std::atomic<U> arriving{0};std::atomic<bool> stop{false};double decompressed=0;
  std::thread worker([&]{auto t0=Clock::now();U result=failedAt;ZSTD_DStream* z=ZSTD_createDStream();
   if(z&&!ZSTD_isError(ZSTD_initDStream(z))){ZSTD_outBuffer out{dictionary_.data(),dictionary_.size(),0};ZSTD_inBuffer in{frame.data(),0,0};
    while(!stop.load(std::memory_order_relaxed)){in.size=std::min<std::size_t>(frame.size(),in.size+(256<<10));auto before=out.pos+in.pos;auto left=ZSTD_decompressStream(z,&out,&in);
     if(ZSTD_isError(left))break;if(!left){if(out.pos==dictionary_.size()&&in.pos==frame.size())result=out.pos;break;}
     if(out.pos+in.pos==before&&in.size==frame.size())break;arriving.store(out.pos,std::memory_order_release);arriving.notify_all();}}
   if(z)ZSTD_freeDStream(z);decompressed=elapsed(t0);arriving.store(result,std::memory_order_release);arriving.notify_all();});
  struct Join{std::thread& w;std::atomic<bool>& stop;~Join(){stop=true;if(w.joinable())w.join();}} join{worker,stop};
  arriving_=&arriving;arrived_=0;
  try{dictionary();need(dictionary_.size());}catch(std::exception const& e){arriving_=nullptr;arrived_=failedAt;stop=true;if(worker.joinable())worker.join();fail(e.what());}
  arriving_=nullptr;arrived_=failedAt;
  worker.join();hasher.join();require(checked,"Section checksum differs");require(arriving.load()==dictionary_.size(),"Zstd decode failed");
  dictionaryReadSeconds_=decompressed;dictionaryParseSeconds_=elapsed(t);dictionaryReady_=true;}
 // Parses the dictionary into its tables, from empty: a load that failed partway (and is tried
 // again by the next read) must not leave entries before the ones parsed again.
 void dictionary(){headers_.clear();values_.clear();hashes_.clear();atoms_.clear();atomHeaders8_.clear();atomHeaders16_.clear();shapes_.clear();
  require(dictionary_.size()<=UINT32_MAX,"Dictionary size");Reader r{dictionary_};need(4);require(r.take(4)=="XHD2","Dictionary magic");auto count=[&](){need(r.pos+32);return r.var(std::min<U>(itemsMax,dictionary_.size()));};
  U n=count();require(n<=65536,"Field header count");headers_.reserve(n);for(U i=0;i<n;++i){auto v=arrivedBytes(r);require(v.size()>=1&&v.size()<=3,"Field header size");headers_.push_back(v);}
  n=count();values_.reserve(n);for(U i=0;i<n;++i){values_.push_back(static_cast<std::uint32_t>(r.pos));arrivedBytes(r);}
  n=count();hashes_.reserve(n);for(U i=0;i<n;++i){need(r.pos+256);Identity v;U mode=r.byte();require(mode<=10,"Hash recipe");if(!mode)v.off=static_cast<std::uint32_t>(r.take(32).data()-dictionary_.data());else {v.off=static_cast<std::uint32_t>(r.pos);r.skip(recipeArgs[mode]+(mode==9));}hashes_.push_back(v);}
  n=count();atoms_.reserve(n);(headers_.size()<=256?atomHeaders16_.clear():atomHeaders8_.clear());if(headers_.size()<=256)atomHeaders8_.reserve(n);else atomHeaders16_.reserve(n);for(U i=0;i<n;++i){need(r.pos+256);Atom a;a.off=static_cast<std::uint32_t>(r.pos);U header=r.var();require(header<headers_.size(),"Atom header");if(headers_.size()<=256)atomHeaders8_.push_back(static_cast<std::uint8_t>(header));else atomHeaders16_.push_back(static_cast<std::uint16_t>(header));U kind=r.var(4);if(kind==4){r.skip();U count=r.var(recordMax/32);need(r.pos+19*count);r.skip(count);}else r.skip(kind==3?3:1);atoms_.push_back(a);}
  n=count();shapes_.reserve(n);for(U i=0;i<n;++i){need(r.pos+32);std::vector<U> shape;U size=r.var(65536);need(r.pos+19*size);shape.reserve(size);for(U j=0;j<size;++j)shape.push_back(r.var());shapes_.push_back(std::move(shape));}r.end();
 }
 static std::size_t txPageBytes(TxPage const& t){return t.raw.capacity()+t.flat.capacity()*sizeof(V)+t.rows.capacity()*sizeof(TxRow)+t.flat.size()*sizeof(V);}
 TxPage& txPage(U i){require(i<txPages_.size(),"Transaction page index");auto& t=txPages_[i];if(t.ready)return t;t.raw=section(1+2*i);
  if(version_==3){require(!t.raw.empty(),"Transaction page mode");if(t.raw[0]==0)t.raw.erase(0,1);else if(t.raw[0]==1)t.raw=transactionColumns(V(t.raw).substr(1));else throw std::runtime_error("Transaction page mode");}
  Reader r{t.raw};auto const& p=get(pages_,i);require(r.var(256)==p.count,"Transaction page count");t.rows.reserve(p.count);t.flat.reserve(p.txCount);
  for(U seq=p.first;seq<p.first+p.count;++seq){require(r.little(4)==seq,"Transaction sequence");TxRow row{seq,r.bytes(),{}};U n=r.var(1000000);row.txs.reserve(n);for(U j=0;j<n;++j){auto tx=r.bytes();require(tx.size()<=recordMax,"Transaction bound");row.txs.push_back(tx);t.flat.push_back(tx);}t.rows.push_back(std::move(row));}r.end();require(t.flat.size()==p.txCount,"Transaction index differs");t.ready=true;heldBytes_+=txPageBytes(t);return t;
 }
 V value(U i) const {require(i<values_.size(),"Reference out of range");Reader r{dictionary_,values_[static_cast<std::size_t>(i)]};return r.bytes();}
 V identity(U i,unsigned depth=0){require(i<hashes_.size(),"Hash reference");require(depth<=2,"Hash recipe depth");auto& h=hashes_[i];U mode=static_cast<unsigned char>(dictionary_[h.off-1]);if(!mode)return V(dictionary_).substr(h.off,32);if(h.memo)return memo_[h.memo-1];
  Reader ar{dictionary_,h.off};U args[6];U nargs=recipeArgs[mode];for(U j=0;j<nargs;++j)args[j]=ar.var(itemsMax);if(mode==9){U page=ar.var(std::numeric_limits<U>::max());require(page>0,"Directory page number");args[nargs++]=page;}S input;
  // Recipes 5-10 are documented with RECIPE_ARGS in history_codec.py.
  if(mode==8){for(U j=0;j<5;++j)require(value(args[j]).size()==(j<4?20U:8U),"Book recipe size");input=S("\0B",2);for(U j=0;j<4;++j)input+=value(args[j]);S out=half(input);out.replace(24,8,S(value(args[4])));return remember(h.memo,std::move(out));}
  if(mode==9){input=S("\0d",2);input+=identity(args[0],depth+1);for(int b=7;b>=0;--b)input+=char((args[1]>>(8*b))&255);}
  else if(mode==10){require(args[0]<count,"Ledger hash recipe range");U seq=first+args[0],index=args[0]/pages_[0].count;auto& tx=txPage(index);input=S("LWR\0",4);input+=get(tx.rows,seq-pages_[index].first).header;}
  else if(mode==1){U t=args[0];auto it=std::upper_bound(pages_.begin(),pages_.end(),t,[](U value,Page const& p){return value<p.txFirst;});require(it!=pages_.begin(),"Transaction hash range");U page=static_cast<U>(it-pages_.begin()-1);auto& tx=txPage(page);input=S("TXN\0",4);input+=get(tx.flat,t-pages_[page].txFirst);}
  else {input=S("\0",1);input+="aorTCO"[mode-2];for(U j=0;j<nargs;++j){auto v=value(args[j]);U size=(mode==3||mode==5||mode==6)&&j==1?4:20;require(v.size()==size,"Object recipe size");input+=v;}}
  return remember(h.memo,half(input));
 }
 // Atom `i`'s header.
 U atomHeader(U i) const{require(i<atoms_.size(),"Reference out of range");return atomHeaders8_.empty()&&!atomHeaders16_.empty()?atomHeaders16_[i]:atomHeaders8_[i];}
 V atom(U i){require(i<atoms_.size(),"Atom reference");auto& a=atoms_[i];if(a.memo)return memo_[a.memo-1];Reader ar{dictionary_,a.off};U header=ar.var();U kind=ar.var(4);S out(get(headers_,header));
  if(kind==0)out+=value(ar.var(itemsMax));
  else if(kind==1)out+=identity(ar.var(itemsMax));
  else if(kind==2){auto v=value(ar.var(itemsMax));require(v.size()==20,"Account size");out+='\x14';out+=v;}
  else if(kind==3){for(U j=0;j<3;++j){auto v=value(ar.var(itemsMax));require(v.size()==(j?20:8),"Amount size");out+=v;}}
  else {auto prefix=value(ar.var(itemsMax));U entries=ar.var(recordMax/32);Reader r{prefix};U a0=r.byte(),n;if(a0<=192)n=a0;else if(a0<=240)n=193+(a0-193)*256+r.byte();else {require(a0<=254,"Vector length");U b=r.byte(),c=r.byte();n=12481+(a0-241)*65536+b*256+c;}r.end();require(n==32*entries,"Vector length differs");out+=prefix;for(U j=0;j<entries;++j)out+=identity(ar.var(itemsMax));}
  require(out.size()<=recordMax,"Atom bound");return remember(a.memo,std::move(out));
 }
 void viewInto(Reader& r,U key,Cache& cache,S& out,U limit){auto const& shape=get(shapes_,r.var());auto mask=r.take((shape.size()+7)/8);auto [it,inserted]=cache.try_emplace(key);auto& old=it->second;if(inserted)old.assign(headers_.size(),std::numeric_limits<U>::max());
  if(!shape.empty()&&shape.size()%8)require((static_cast<unsigned char>(mask.back())>>(shape.size()%8))==0,"Unused view bits");
  for(U j=0;j<shape.size();++j){U hid=shape[j];require(hid<old.size(),"Shape header");if(static_cast<unsigned char>(mask[j/8])&(1<<(j%8))){U aid=r.var();require(atomHeader(aid)==hid,"Field shape differs");old[hid]=aid;}require(old[hid]!=std::numeric_limits<U>::max(),"Missing page anchor");out+=atom(old[hid]);require(out.size()<=limit,"View expansion bound");}
 }
 S view(Reader& r,U key,Cache& cache,U budget){S out;out.reserve(budget);viewInto(r,key,cache,out,budget);return out;}
 void nodeInto(Reader& r,Cache& cache,S& out,U limit,unsigned depth=0){require(depth<=64,"Node depth");U tag=r.byte();if(!tag){out+=atom(r.var());require(out.size()<=limit,"Node bound");return;}require(tag==1||tag==2,"Node tag");auto h=get(headers_,r.var());auto end=r.bytes();require(end.size()>=1&&end.size()<=3&&out.size()+h.size()+end.size()<=limit,"Container bound");out+=h;
  if(tag==2){U key=r.var();get(hashes_,key);U reset=r.byte();require(reset<=1,"Reset flag");if(reset)cache.erase(key);viewInto(r,key,cache,out,limit-end.size());}
  else {U count=r.var(std::min<U>(1000000,r.data.size()));for(U i=0;i<count;++i)nodeInto(r,cache,out,limit-end.size(),depth+1);}
  out+=end;
 }
 S metadata(Reader& r,Cache& cache,U length){U tag=r.byte();S out;out.reserve(length);if(!tag)out=r.bytes();else {require(tag==1,"Metadata tag");U count=r.var(65536);for(U i=0;i<count;++i)nodeInto(r,cache,out,length);}require(out.size()==length,"Metadata size");return out;}
 // Parse-only twins of viewInto/nodeInto/metadata: they update the page's view cache exactly as a
 // full decode does but build no bytes, so object patches decode without materializing metadata.
 void skipView(Reader& r,U key,Cache& cache){auto const& shape=get(shapes_,r.var());auto mask=r.take((shape.size()+7)/8);auto [it,inserted]=cache.try_emplace(key);auto& old=it->second;if(inserted)old.assign(headers_.size(),std::numeric_limits<U>::max());
  if(!shape.empty()&&shape.size()%8)require((static_cast<unsigned char>(mask.back())>>(shape.size()%8))==0,"Unused view bits");
  for(U j=0;j<shape.size();++j){U hid=shape[j];require(hid<old.size(),"Shape header");if(static_cast<unsigned char>(mask[j/8])&(1<<(j%8))){U aid=r.var();require(atomHeader(aid)==hid,"Field shape differs");old[hid]=aid;}require(old[hid]!=std::numeric_limits<U>::max(),"Missing page anchor");}
 }
 void skipNode(Reader& r,Cache& cache,unsigned depth=0){require(depth<=64,"Node depth");U tag=r.byte();if(!tag){get(atoms_,r.var());return;}require(tag==1||tag==2,"Node tag");get(headers_,r.var());auto end=r.bytes();require(end.size()>=1&&end.size()<=3,"Container bound");
  if(tag==2){U key=r.var();get(hashes_,key);U reset=r.byte();require(reset<=1,"Reset flag");if(reset)cache.erase(key);skipView(r,key,cache);}
  else {U count=r.var(std::min<U>(1000000,r.data.size()));for(U i=0;i<count;++i)skipNode(r,cache,depth+1);}
 }
 void skipMetadata(Reader& r,Cache& cache){U tag=r.byte();if(!tag){r.bytes();return;}require(tag==1,"Metadata tag");U count=r.var(65536);for(U i=0;i<count;++i)skipNode(r,cache);}
 // One ledger's records parsed but not built: they only update the page's view cache.
 // `row` (when its transaction section is decoded) checks the metadata count the history section gives.
 void skipRow(Reader& r,Cache& cache,TxRow const* row){U n=r.var(1000000);require(!row||n==row->txs.size(),"Metadata count");for(U j=0;j<n;++j){r.var();r.var(recordMax);skipMetadata(r,cache);}U count=r.var(std::min<U>(1000000,r.data.size()));
  for(U j=0;j<count;++j){U kid=r.var(),len=r.var(recordMax);get(hashes_,kid);if(!len)cache.erase(kid);else {U tag=r.byte();if(!tag)r.bytes();else {require(tag==1,"Object tag");skipView(r,kid,cache);}}}}
 // One ledger's record; `patchesOnly` leaves its metadata empty. Either way the view cache is updated alike.
 S buildRow(Reader& r,Cache& cache,TxRow const& row,bool patchesOnly){require(r.var(1000000)==row.txs.size(),"Metadata count");std::vector<S> metas(row.txs.size());std::vector<bool> seen(row.txs.size());
  for(U j=0;j<row.txs.size();++j){U at=r.var(),len=r.var(recordMax);require(at<metas.size()&&!seen[at],"Transaction permutation");seen[at]=true;if(patchesOnly)skipMetadata(r,cache);else metas[at]=metadata(r,cache,len);}
  S out;le(out,row.seq,4);rec(out,row.header);le(out,row.txs.size(),4);for(U j=0;j<row.txs.size();++j){rec(out,row.txs[j]);rec(out,metas[j]);require(out.size()<=sectionMax,"Ledger transaction bound");}
  patchesInto(r,cache,out);return out;}
 // A ledger's object changes, appended to `out` as buildRow lays them out.
 void patchesInto(Reader& r,Cache& cache,S& out){U count=r.var(std::min<U>(1000000,r.data.size()));le(out,count,4);S prior;
  for(U j=0;j<count;++j){U kid=r.var(),len=r.var(recordMax);S key(identity(kid));require(prior.empty()||key>prior,"Patch order");prior=key;S value;if(!len)cache.erase(kid);else {U tag=r.byte();if(!tag)value=r.bytes();else {require(tag==1,"Object tag");value=view(r,kid,cache,len);}require(value.size()==len,"Object size");}out+=key;rec(out,value);require(out.size()<=sectionMax,"Ledger bound");}}
 // An object-only record: the ledger's sequence, no header, no transactions, and its object changes. The
 // page's transaction section is not needed: the metadata count comes from the history section, whose
 // checksum was checked when it was decompressed (and is checked against `row` when that is decoded
 // anyway). The view cache is updated as buildRow updates it.
 S buildPatches(Reader& r,Cache& cache,U sequence,TxRow const* row){U n=r.var(1000000);require(!row||n==row->txs.size(),"Metadata count");std::vector<bool> seen(n);
  for(U j=0;j<n;++j){U at=r.var();r.var(recordMax);require(at<n&&!seen[at],"Transaction permutation");seen[at]=true;skipMetadata(r,cache);}
  S out;le(out,sequence,4);rec(out,V());le(out,0,4);patchesInto(r,cache,out);return out;}
 // `patchesOnly` leaves every metadata empty and may stop after ledger `upto`. It cannot check the canonical page digest, nor
 // can `verify` false: a caller that skips it must have verified this page with a full decode before.
 std::vector<std::pair<U,S>> page(U index,bool patchesOnly=false,U upto=std::numeric_limits<U>::max(),bool verify=true){loadDictionary();auto& tx=txPage(index);auto raw=section(2+2*index);Reader r{raw};auto const& p=get(pages_,index);Cache cache;cache.reserve(8192);std::vector<std::pair<U,S>> records;U total=0;require(r.var(256)==p.count,"History page count");
  for(auto const& row:tx.rows){S out=buildRow(r,cache,row,patchesOnly);total+=out.size();require(total<=sectionMax,"Page decoded bound");records.emplace_back(row.seq,std::move(out));if(patchesOnly&&row.seq>=upto)return records;}
  r.end();require(patchesOnly||!verify||sha(body(records))==p.digest,"Canonical page digest differs");return records;
 }
 // Where one-ledger decodes left off: a page's history section (shared by the cursors on that page), the view cache after
 // the ledger built and the next ledger's position. A ledger resumes from the cursor on its page nearest before it, else
 // starts a cursor at the page's start, replacing the least recently used one past `cursorsMax`. Requests arrive in no
 // particular order, so several resume points serve more of them than one.
 struct Cursor {U index=std::numeric_limits<U>::max(),next=0,used=0;std::size_t pos=0;std::shared_ptr<S const> raw;Cache cache;};
 static constexpr std::size_t cursorsMax=4;
 std::vector<Cursor> cursors_;U cursorUse_=0;
 S ledger(U sequence,bool patchesOnly){loadDictionary();U index=pageOf(sequence);TxPage* tx=patchesOnly&&!get(txPages_,index).ready?nullptr:&txPage(index);auto const& p=get(pages_,index);U at=sequence-p.first;
  Cursor* c=nullptr;std::shared_ptr<S const> raw;
  for(auto& k:cursors_)if(k.index==index){if(!raw)raw=k.raw;if(k.next<=at&&(!c||k.next>c->next))c=&k;}
  if(!c){
   if(cursors_.size()<cursorsMax)c=&cursors_.emplace_back();else c=&*std::min_element(cursors_.begin(),cursors_.end(),[](Cursor const& a,Cursor const& b){return a.used<b.used;});
   *c=Cursor{};c->raw=raw?raw:std::make_shared<S const>(section(2+2*index));Reader r{*c->raw};require(r.var(256)==p.count,"History page count");c->pos=r.pos;c->index=index;c->cache.reserve(8192);}
  c->used=++cursorUse_;
  try{Reader r{*c->raw,c->pos};for(U i=c->next;i<at;++i)skipRow(r,c->cache,tx?&get(tx->rows,i):nullptr);S out=patchesOnly?buildPatches(r,c->cache,sequence,tx?&get(tx->rows,at):nullptr):buildRow(r,c->cache,get(tx->rows,at),false);c->pos=r.pos;c->next=at+1;return out;}
  catch(...){*c=Cursor{};throw;}
 }
public:
 U first=0,count=0,transactions=0;
 // `trusted` skips the whole-segment digest when the caller authenticated the bytes already (a query
 // server over a verified cache); each section is still checked when decoded. The dictionary is then
 // decoded on first use, so header and transaction reads never pay for it.
 Archive(Archive const&)=delete;Archive& operator=(Archive const&)=delete;Archive(Archive&&)=default;Archive& operator=(Archive&&)=default;
 explicit Archive(V data,bool trusted=false):data_(data){require(data.size()>=82&&data.size()<=segmentMax,"Segment bound");require(trusted||sha(data.substr(0,data.size()-32))==data.substr(data.size()-32),"Segment checksum differs");Reader r{data};require(r.take(4)=="XRPH","Segment magic");version_=r.little(2);first=r.little(4);count=r.little(4);U block=r.little(2),n=r.little(2);digest_=r.take(32);require((version_==2||version_==3)&&count>=1&&count<=4096&&block>=1&&block<=256&&n==(count+block-1)/block,"Segment header");U seq=first,tx=0;
  for(U i=0;i<n;++i){Page p{r.little(4),r.little(4),r.little(4),r.little(4),S(r.take(32))};require(p.first==seq&&p.count==std::min(block,first+count-seq)&&p.txFirst==tx&&p.txCount<=1000000,"Page index");seq+=p.count;tx+=p.txCount;pages_.push_back(std::move(p));}transactions=tx;txPages_.resize(n);
  U start=r.pos+(2*n+1)*40,total=0;for(U i=0;i<2*n+1;++i){U size=r.little(4),length=r.little(4);S digest(r.take(32));require(size<=sectionMax&&start+length<=data.size()-32,"Section bound");sections_.push_back({start,length,size,std::move(digest)});start+=length;total+=size;}require(start==data.size()-32&&total<=segmentMax,"Segment coverage");sectionChecked_=std::make_unique<std::atomic<bool>[]>(sections_.size());if(!trusted)loadDictionary();
 }
 S restore(){std::vector<std::pair<U,S>> all;all.reserve(count);U total=0;for(U i=0;i<pages_.size();++i){auto rows=page(i);for(auto& row:rows){total+=row.second.size()+12;require(total<=segmentMax,"Canonical segment bound");all.push_back(std::move(row));}}S result=body(all);require(sha(result)==digest_,"Canonical segment digest differs");return result;}
 U pageFirst(U sequence) const {require(sequence>=first&&sequence<first+count,"Ledger outside segment");return first+(sequence-first)/pages_[0].count*pages_[0].count;}
 std::vector<std::pair<U,S>> readPage(U sequence){pageFirst(sequence);return page((sequence-first)/pages_[0].count);}
 U pages() const {return pages_.size();}
 U pageOf(U sequence) const {return (pageFirst(sequence)-first)/pages_[0].count;}
 U pageLedgers() const {return pages_[0].count;}
 void prepare(){loadDictionary();}
 std::pair<double,double> dictionarySeconds() const{return {dictionaryReadSeconds_,dictionaryParseSeconds_};}
 // Bytes the loaded dictionary holds (its section and the tables parsed from it), for a cache's accounting.
 std::size_t dictionaryBytes() const{if(!dictionaryReady_)return 0;std::size_t n=dictionary_.size()+headers_.capacity()*sizeof(V)+values_.capacity()*sizeof(std::uint32_t)+hashes_.capacity()*sizeof(Identity)+atoms_.capacity()*sizeof(Atom)+atomHeaders8_.capacity()+atomHeaders16_.capacity()*2;for(auto const& s:shapes_)n+=sizeof(s)+s.capacity()*sizeof(U);return n;}
 // Renders every memoized value up front (the dictionary's hashes and atoms): decoding pages then
 // only reads shared state, so a segment's pages can decode on several threads at once.
 void prepareAll(){loadDictionary();for(U i=0;i<hashes_.size();++i)identity(i);for(U i=0;i<atoms_.size();++i)atom(i);}
 // `verify` false skips the canonical page digest, for a caller that checked it when the segment was indexed.
 std::vector<std::pair<U,S>> decodePage(U index,bool patchesOnly=false,U upto=std::numeric_limits<U>::max(),bool verify=true){require(index<pages_.size(),"Page index");return page(index,patchesOnly,upto,verify);}
 // One ledger's record (unverified, as with `verify` false): the page's earlier ledgers are parsed but not built, from where
 // the previous one-ledger decode stopped when it was an earlier ledger of the same page. With `patchesOnly` the record
 // has no header and no transactions (only its sequence and object changes), and the transaction section is not read.
 S decodeLedger(U sequence,bool patchesOnly=false){return ledger(sequence,patchesOnly);}
 // The object changes of a ledger record (from their count on), as decodePage and decodeLedger lay them out.
 static V patchesOf(V record){Reader r{record};r.take(4);r.take(r.little(4));for(U n=r.little(4),i=0;i<n;++i){r.take(r.little(4));r.take(r.little(4));}return record.substr(r.pos);}
 // Memory the one-ledger cursor holds: a page's history section and its view cache.
 std::size_t cursorBytes() const{std::size_t n=0;std::vector<S const*> seen;for(auto const& c:cursors_){if(c.raw&&std::find(seen.begin(),seen.end(),c.raw.get())==seen.end()){seen.push_back(c.raw.get());n+=c.raw->capacity();}n+=c.cache.size()*(64+headers_.size()*sizeof(U));}return n;}
 // Drops a page's decoded transaction section; a one-pass scan calls it to stay within one page.
 void releasePage(U index){require(index<txPages_.size(),"Page index");if(txPages_[index].ready)heldBytes_-=txPageBytes(txPages_[index]);txPages_[index]=TxPage{};}
 // Memory the archive holds now, for a cache's accounting: the dictionary, the values rendered from it
 // (kept for the next pages), the transaction sections read, and the one-ledger cursors.
 std::size_t heldBytes() const{return dictionaryBytes()+heldBytes_+cursorBytes();}
 // What the archive holds, by part, for a cache's statistics: the dictionary's section and the tables
 // parsed from it, the values rendered from it (memos), the transaction sections read, and the cursors.
 struct Memory {std::size_t dictionary=0,tables=0,memos=0,memoCount=0,transactionPages=0,cursors=0,headers=0,values=0,hashes=0,atoms=0,shapes=0,shapeItems=0,shapeCount=0;};
 Memory memory() const{Memory m;if(dictionaryReady_){m.dictionary=dictionary_.size();m.tables=dictionaryBytes()-dictionary_.size();
   m.headers=headers_.capacity()*sizeof(V);m.values=values_.capacity()*sizeof(std::uint32_t);m.hashes=hashes_.capacity()*sizeof(Identity);m.atoms=atoms_.capacity()*sizeof(Atom)+atomHeaders8_.capacity()+atomHeaders16_.capacity()*2;
   for(auto const& sh:shapes_){m.shapes+=sizeof(sh)+sh.capacity()*sizeof(U);m.shapeItems+=sh.size();}m.shapeCount=shapes_.size();}
  for(auto const& v:memo_)m.memos+=sizeof(S)+v.capacity();m.memoCount=memo_.size();
  for(auto const& t:txPages_)if(t.ready)m.transactionPages+=txPageBytes(t);m.cursors=cursorBytes();return m;}
 // Headers and transaction blobs come from the transaction sections alone, without the dictionary.
 V header(U sequence){U i=pageOf(sequence);return get(txPage(i).rows,sequence-pages_[i].first).header;}
 std::vector<V> const& transactionsOf(U sequence){U i=pageOf(sequence);return get(txPage(i).rows,sequence-pages_[i].first).txs;}
 S read(U sequence){auto rows=readPage(sequence);for(auto& [seq,row]:rows)if(seq==sequence)return std::move(row);throw std::runtime_error("Missing ledger");}
};
inline S load(S const& path){require(std::filesystem::file_size(path)<=segmentMax,"Input file bound");std::ifstream f(path,std::ios::binary);require(bool(f),"Cannot open input");S s((std::istreambuf_iterator<char>(f)),{});require(!f.bad(),"Read failed");return s;}
inline void save(S const& path,V bytes){std::ofstream f(path,std::ios::binary|std::ios::trunc);f.write(bytes.data(),bytes.size());require(bool(f),"Output write failed");}
}

