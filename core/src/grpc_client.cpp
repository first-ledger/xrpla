#include <xrpla/core/operations.hpp>
#include <xrpl/proto/org/xrpl/rpc/v1/xrp_ledger.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <cstdlib>
#include <iostream>

namespace xrpla::core {
namespace api = org::xrpl::rpc::v1;
struct GrpcLedgerClient::Impl {
    std::unique_ptr<api::XRPLedgerAPIService::Stub> stub;
};
GrpcLedgerClient::GrpcLedgerClient(std::string const& endpoint) : impl_(std::make_unique<Impl>()) {
    bool tls = endpoint.starts_with("grpcs://");
    if (!tls && !endpoint.starts_with("grpc://")) throw Error("gRPC source must start with grpc:// or grpcs://");
    auto address = endpoint.substr(tls ? 8 : 7);
    if (address.empty() || address.find('/') != std::string::npos) throw Error("Invalid gRPC source address");
    grpc::ChannelArguments args; args.SetMaxReceiveMessageSize(512 * 1024 * 1024);
    grpc::SslCredentialsOptions ssl;
    if (tls) {
        if (auto bundle = std::getenv("SSL_CERT_FILE"); bundle && *bundle) ssl.pem_root_certs = read_file(bundle, 4 * 1024 * 1024);
        else if (fs::exists("/etc/ssl/certs/ca-certificates.crt")) ssl.pem_root_certs = read_file("/etc/ssl/certs/ca-certificates.crt", 4 * 1024 * 1024);
    }
    auto credentials = tls ? grpc::SslCredentials(ssl) : grpc::InsecureChannelCredentials();
    impl_->stub = api::XRPLedgerAPIService::NewStub(grpc::CreateCustomChannel(address, credentials, args));
}
GrpcLedgerClient::~GrpcLedgerClient() = default;
namespace {
// xrpld's gRPC header is addRaw(includeHash=true): the 118 bytes, then the ledger's hash.
Header wire_header(std::string const& wire) {
    if (wire.size() != 150) throw Error("Invalid gRPC ledger header length");
    auto header = decode_header(wire.substr(0, 118));
    if (to_hex(wire.substr(118)) != header.hash) throw Error("gRPC ledger header hash mismatch");
    return header;
}
}
Header GrpcLedgerClient::validated() const {
    api::GetLedgerRequest request;
    request.mutable_ledger()->set_shortcut(api::LedgerSpecifier::SHORTCUT_VALIDATED);
    request.set_user("xrpla");
    api::GetLedgerResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds{30});
    auto result = impl_->stub->GetLedger(&context, request, &response);
    if (!result.ok()) throw Error("gRPC GetLedger (validated) failed: " + result.error_message());
    last_unlimited_ = response.is_unlimited();
    if (!response.validated()) throw Error("gRPC source tip is not validated");
    return wire_header(response.ledger_header());
}
Header GrpcLedgerClient::header(std::uint32_t sequence) const {
    api::GetLedgerRequest request;
    request.mutable_ledger()->set_sequence(sequence);
    request.set_user("xrpla");
    api::GetLedgerResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds{30});
    auto result = impl_->stub->GetLedger(&context, request, &response);
    if (!result.ok()) throw Error("gRPC GetLedger (header) failed: " + result.error_message());
    last_unlimited_ = response.is_unlimited();
    if (!response.validated()) throw Error("gRPC source ledger " + std::to_string(sequence) + " is not validated");
    auto header = wire_header(response.ledger_header());
    if (header.sequence != sequence) throw Error("gRPC source returned a different ledger sequence");
    return header;
}
std::pair<Header, Json> GrpcLedgerClient::ledger(std::uint32_t sequence) const {
    api::GetLedgerRequest request;
    request.mutable_ledger()->set_sequence(sequence);
    request.set_transactions(true); request.set_expand(true); request.set_get_objects(true);
    // xrpld exempts a request from its resource limits when it names a user and comes from a
    // [port_grpc] secure_gateway address (Clio's ETL does the same); is_unlimited says so.
    request.set_user("xrpla");
    api::GetLedgerResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds{60});
    auto result = impl_->stub->GetLedger(&context, request, &response);
    if (!result.ok()) throw Error("gRPC GetLedger failed: " + result.error_message());
    last_unlimited_ = response.is_unlimited();
    if (!response.validated() || !response.objects_included() || !response.skiplist_included())
        throw Error("gRPC source omitted validated ledger data, transactions, full diffs or skip lists");
    auto header = wire_header(response.ledger_header());
    if (header.sequence != sequence) throw Error("gRPC source returned a different ledger sequence");
    Json document{{"transactions", Json::array()}, {"diff", Json::array()}};
    // xrpld leaves the transactions oneof unset for a genuinely empty ledger, and also when it
    // cannot decode one of the ledger's transactions (it then sends none of them).
    if (!response.has_transactions_list() && header.transaction_hash != std::string(64, '0')) document["transactions_omitted"] = true;
    for (auto const& tx : response.transactions_list().transactions())
        document["transactions"].push_back({{"tx_blob", to_hex(tx.transaction_blob())}, {"meta", to_hex(tx.metadata_blob())}});
    for (auto const& object : response.ledger_objects().objects())
        document["diff"].push_back({{"object_id", to_hex(object.key())}, {"object", to_hex(object.data())}});
    return {header, document};
}
std::pair<std::vector<ObjectRecord>, Bytes> GrpcLedgerClient::state_page(std::uint32_t sequence, Bytes const& marker, Bytes const& end_marker) const {
    api::GetLedgerDataRequest request;
    request.mutable_ledger()->set_sequence(sequence);
    if (!marker.empty()) request.set_marker(marker);
    if (!end_marker.empty()) request.set_end_marker(end_marker);
    request.set_user("xrpla");
    api::GetLedgerDataResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds{120});
    auto result = impl_->stub->GetLedgerData(&context, request, &response);
    if (!result.ok()) throw Error("gRPC GetLedgerData failed: " + result.error_message());
    last_unlimited_ = response.is_unlimited();
    // xrpld leaves ledger_index unset in these answers.
    if (response.ledger_index() && response.ledger_index() != sequence)
        throw Error("gRPC source returned state of ledger " + std::to_string(response.ledger_index()) + " for " + std::to_string(sequence) +
                    " (hash " + response.ledger_hash().substr(0, 8) + ", " + std::to_string(response.ledger_objects().objects_size()) + " objects)");
    std::vector<ObjectRecord> objects;
    objects.reserve(response.ledger_objects().objects_size());
    for (auto const& object : response.ledger_objects().objects()) objects.push_back({object.key(), object.data()});
    return {std::move(objects), response.marker()};
}
Json probe_grpc(std::string const& source, std::uint32_t first, std::uint32_t count, unsigned pages) {
    GrpcLedgerClient client(source);
    using clock = std::chrono::steady_clock;
    auto seconds = [](clock::time_point began) { return std::chrono::duration<double>(clock::now() - began).count(); };
    Json result{{"source", source}, {"first", first}};
    auto began = clock::now();
    std::uint64_t transactions = 0, objects = 0, bytes = 0;
    for (std::uint32_t seq = first; seq < first + count; ++seq) {
        auto [header, document] = client.ledger(seq);
        transactions += document.at("transactions").size(); objects += document.at("diff").size();
        bytes += document.dump().size() / 2;
    }
    auto ledger_seconds = seconds(began);
    result["unlimited"] = client.unlimited();
    result["ledgers"] = {{"count", count}, {"seconds", ledger_seconds}, {"per_second", count / std::max(ledger_seconds, 1e-9)},
                         {"transactions", transactions}, {"changed_objects", objects}, {"approx_mb_per_second", bytes / 1e6 / std::max(ledger_seconds, 1e-9)}};
    began = clock::now();
    Bytes marker;
    std::uint64_t state_objects = 0;
    unsigned done = 0;
    try {
        for (; done < pages; ++done) {
            auto [page, next] = client.state_page(first, marker);
            state_objects += page.size();
            marker = std::move(next);
            if (marker.empty()) { ++done; break; }
        }
    } catch (std::exception const& error) { result["state_error"] = error.what(); }
    auto state_seconds = seconds(began);
    result["state"] = {{"pages", done}, {"objects", state_objects}, {"seconds", state_seconds},
                       {"objects_per_second", state_objects / std::max(state_seconds, 1e-9)}};
    return result;
}
}
