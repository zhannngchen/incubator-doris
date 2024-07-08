// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/delete_bitmap_calculator.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/primary_key_index.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/exec_env.h"

namespace doris {
using namespace ErrorCode;

static std::string kSegmentDir = "./ut_dir/delete_bitmap_calculator_test";

using Generator = std::function<void(size_t rid, int cid, RowCursorCell& cell)>;

struct ColumnSchema {
    std::string name;
    std::string type;
    bool is_nullable;
    bool is_key;
};

class DeleteBitmapCalculatorTest1 : public testing::Test {
public:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kSegmentDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kSegmentDir);
        ASSERT_TRUE(st.ok()) << st;
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions {}));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kSegmentDir).ok());
    }

    TabletSchemaSPtr create_schema(const std::vector<ColumnSchema>& columns) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        auto unique_id = 0;
        tablet_schema_pb.set_keys_type(UNIQUE_KEYS);

        for (const auto& col : columns) {
            ColumnPB* column = tablet_schema_pb.add_column();
            column->set_unique_id(unique_id++);
            column->set_name(col.name);

            // 转换类型
            if (col.type.find("DECIMAL") != std::string::npos) {
                column->set_type("DECIMAL");
                column->set_length(12); // 可以根据实际情况调整
            } else if (col.type == "INT") {
                    column->set_type("INT");
                    column->set_length(4);  // 可以根据实际情况调整
            } else {
                column->set_type(col.type);
                column->set_length(8);  // 默认长度，具体根据实际情况调整
            }

            column->set_is_nullable(col.is_nullable);
            column->set_aggregation("NONE");  // 假设所有列的聚合方式均为 NONE
            column->set_is_key(col.is_key);        // 假设所有列初始值均不是key
        }
        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    void load_segments(TabletSchemaSPtr tablet_schema, RowsetId rowset_id, size_t num_segments,
                       std::vector<std::shared_ptr<segment_v2::Segment>>& segments) {
        for (int segment_id = 0; segment_id < num_segments; segment_id++) {
            std::string filename = fmt::format("{}_{}.dat", rowset_id.to_string(), segment_id);
            std::string path = fmt::format("{}/{}", kSegmentDir, filename);
            auto fs = io::global_local_filesystem();
            auto segment = segments[segment_id];
            auto st = segment_v2::Segment::open(fs, path, segment_id, rowset_id, tablet_schema,
                                                io::FileReaderOptions {}, &segment);
            EXPECT_TRUE(st.ok());
        }
    }

    void run_test(size_t const num_segments) {
        SegmentWriterOptions opts;
        opts.enable_unique_key_merge_on_write = true;

        std::vector<ColumnSchema> columns = {
                {"ss_sold_date_sk", "BIGINT", true, true},
                {"ss_item_sk", "BIGINT", true, true},
                {"ss_ticket_number", "BIGINT", true, true},
                {"ss_sold_time_sk", "BIGINT", true, false},
                {"ss_customer_sk", "BIGINT", true, false},
                {"ss_cdemo_sk", "BIGINT", true, false},
                {"ss_hdemo_sk", "BIGINT", true, false},
                {"ss_addr_sk", "BIGINT", true, false},
                {"ss_store_sk", "BIGINT", true, false},
                {"ss_promo_sk", "BIGINT", true, false},
                {"ss_quantity", "INT", true, false},
                {"ss_wholesale_cost", "DECIMAL(7,2)", true, false},
                {"ss_list_price", "DECIMAL(7,2)", true, false},
                {"ss_sales_price", "DECIMAL(7,2)", true, false},
                {"ss_ext_discount_amt", "DECIMAL(7,2)", true, false},
                {"ss_ext_sales_price", "DECIMAL(7,2)", true, false},
                {"ss_ext_wholesale_cost", "DECIMAL(7,2)", true, false},
                {"ss_ext_list_price", "DECIMAL(7,2)", true, false},
                {"ss_ext_tax", "DECIMAL(7,2)", true, false},
                {"ss_coupon_amt", "DECIMAL(7,2)", true, false},
                {"ss_net_paid", "DECIMAL(7,2)", true, false},
                {"ss_net_paid_inc_tax", "DECIMAL(7,2)", true, false},
                {"ss_net_profit", "DECIMAL(7,2)", true, false}
        };

        RowsetId rowset_id;
        rowset_id.init("020000000000492f26418bf712a4e8f414395880f176798a");
        TabletSchemaSPtr tablet_schema = create_schema(columns);
        std::vector<std::shared_ptr<segment_v2::Segment>> segments(num_segments);
        load_segments(tablet_schema, rowset_id, num_segments, segments);

        // find the location of rows to be deleted using `MergeIndexDeleteBitmapCalculator`
        // and the result is `result1`
        MergeIndexDeleteBitmapCalculator calculator;
        size_t seq_col_len = 0;
        ASSERT_TRUE(calculator.init(rowset_id, segments, seq_col_len).ok());
        DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(0);
        ASSERT_TRUE(calculator.calculate_all(delete_bitmap).ok());

        std::set<std::pair<size_t, size_t>> result1;
        for (auto [bitmap_key, row_ids] : delete_bitmap->delete_bitmap) {
            auto segment_id = std::get<1>(bitmap_key);
            for (auto row_id : row_ids) {
                result1.emplace(segment_id, row_id);
            }
        }
        LOG(INFO) << fmt::format("result1.size(): {}", result1.size());
        // if result1 is equal to result2,
        // we assume the result of `MergeIndexDeleteBitmapCalculator` is correct.
        ASSERT_EQ(result1.size(), 1);
    }
};

TEST_F(DeleteBitmapCalculatorTest1, stress_replay) {
    run_test(4);
}

} // namespace doris
