#include <iostream>

#include <arrow/json/api.h>
#include <arrow/io/file.h>
#include <arrow/io/stdio.h>
#include <arrow/io/interfaces.h>
#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/table.h>

#include <parquet/stream_writer.h>
#include <parquet/arrow/schema.h>

#include <parquet/arrow/writer.h>
#include <arrow/util/type_fwd.h>

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

int main() {
    arrow::Status st;
    auto read_options = arrow::json::ReadOptions::Defaults();
    auto parse_options = arrow::json::ParseOptions::Defaults();
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto input = std::shared_ptr<arrow::io::InputStream>(new StdinStream());
    auto output = std::shared_ptr<arrow::io::OutputStream>(new arrow::io::StdoutStream());

    auto result = arrow::json::StreamingReader::Make(
        input,
        read_options,
        parse_options);

    if (!result.ok()) {
        std::cerr << "Cannot create streaming json reader\n";
        return -1;
    }
    auto reader = *result;

    using parquet::ArrowWriterProperties;
    using parquet::WriterProperties;

    // Choose compression
    std::shared_ptr<WriterProperties> props =
        WriterProperties::Builder().compression(arrow::Compression::ZSTD)->build();

    // Opt to store Arrow schema for easier reads back into Arrow
    std::shared_ptr<ArrowWriterProperties> arrow_props =
        ArrowWriterProperties::Builder().store_schema()->build();

    // Create a writer
    std::unique_ptr<parquet::arrow::FileWriter> writer = * parquet::arrow::FileWriter::Open(*reader->schema().get(),
                                                 arrow::default_memory_pool(), output,
                                                 props, arrow_props);

    // Write each batch as a row_group
    for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *reader) {
        auto batch = *maybe_batch;
        auto table = *arrow::Table::FromRecordBatches(batch->schema(), {batch});
        if (!writer->WriteTable(*table.get(), batch->num_rows()).ok()) {
            std::cerr << "Cannot write\n";
        }
    }

    // Write file footer and close
    if (!writer->Close().ok()) {
        std::cerr << "Cannot close\n";
    }

    return 0;
}

