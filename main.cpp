#include <iostream>

#include <arrow/json/api.h>
#include <arrow/io/file.h>
#include <arrow/io/stdio.h>
#include <arrow/io/interfaces.h>
#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <arrow/util/type_fwd.h>
#include <arrow/util/compression.h>

#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>

namespace {

class StdinStream : public arrow::io::InputStream {
 public:
  StdinStream();
  ~StdinStream() override {}

  arrow::Status Close() override;
  bool closed() const override;

  arrow::Result<int64_t> Tell() const override;

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

 private:
  int64_t pos_;
};

StdinStream::StdinStream() : pos_(0) { set_mode(arrow::io::FileMode::READ); }

arrow::Status StdinStream::Close() { return arrow::Status::OK(); }

bool StdinStream::closed() const { return false; }

arrow::Result<int64_t> StdinStream::Tell() const { return pos_; }

arrow::Result<int64_t> StdinStream::Read(int64_t nbytes, void* out) {
  std::cin.read(reinterpret_cast<char*>(out), nbytes);
  nbytes = std::cin.gcount();
  pos_ += nbytes;
  return nbytes;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> StdinStream::Read(int64_t nbytes) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
  ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
  buffer->ZeroPadding();
  return std::move(buffer);
}

void usage(const char* name) {
    printf("%s [--date-fields field1,field2,...] [--max-row-group-length 1000] [--compression snappy]\n", name);
}

} /* namespace { */

int main(int argc, char** argv) {
    arrow::Status st;
    auto read_options = arrow::json::ReadOptions::Defaults();
    auto parse_options = arrow::json::ParseOptions::Defaults();
    int max_row_group_length = 1000000;
    auto compression = * arrow::util::Codec::GetCompressionType("zstd");

    arrow::FieldVector fields;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--date-fields") && i < argc-1) {
            const char* sep = ","; i++;
            for (char* tok = strtok(argv[i], sep); tok; tok = strtok(nullptr, sep)) {
                fields.push_back(std::shared_ptr<arrow::Field>(new arrow::Field(tok, std::shared_ptr<arrow::DataType>(new arrow::Date32Type))));
            }
        } else if (!strcmp(argv[i], "--max-row-group-length") && i < argc-1) {
            max_row_group_length = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--compression") && i < argc-1) {
            compression = * arrow::util::Codec::GetCompressionType(argv[++i]);
        } else {
            printf("Unknown arg: %s\n", argv[i]);
            usage(argv[0]); return -1;
        }
    }

    /*
    for (auto& field : {"l_commitdate", "l_shipdate", "l_receiptdate", "o_orderdate"}) {
        fields.push_back(std::shared_ptr<arrow::Field>(new arrow::Field(field, std::shared_ptr<arrow::DataType>(new arrow::Date32Type))));
    }
    */

    if (!fields.empty()) {
        parse_options.explicit_schema.reset(new arrow::Schema(fields));
    }

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto input = std::shared_ptr<arrow::io::InputStream>(new StdinStream());
    auto output = std::shared_ptr<arrow::io::OutputStream>(new arrow::io::StdoutStream());

    auto reader = * arrow::json::StreamingReader::Make(
        input,
        read_options,
        parse_options);

    using parquet::ArrowWriterProperties;
    using parquet::WriterProperties;

    std::shared_ptr<WriterProperties> props =
        WriterProperties::Builder()
            .memory_pool(pool)
            ->compression(compression)
            ->max_row_group_length(max_row_group_length)
            ->build();

    std::shared_ptr<ArrowWriterProperties> arrow_props =
        ArrowWriterProperties::Builder()
            .store_schema()
            ->build();

    std::unique_ptr<parquet::arrow::FileWriter> writer = * parquet::arrow::FileWriter::Open(*reader->schema().get(),
        pool, output,
        props, arrow_props);

    for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *reader) {
        auto batch = *maybe_batch;
        if (!writer->WriteRecordBatch(*batch.get()).ok()) {
            std::cerr << "Cannot write\n";
        }
    }

    if (!writer->Close().ok()) {
        std::cerr << "Cannot close\n";
    }

    return 0;
}

