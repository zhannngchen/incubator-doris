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

#include <memory>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/phdr_cache.h"
#include "common/status.h"
#include "gtest/gtest.h"
#include "olap/page_cache.h"
#include "olap/segment_loader.h"
#include "olap/tablet_column_object_pool.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "service/http_service.h"
#include "test_util.h"
#include "testutil/http_utils.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "vec/exec/format/orc/orc_memory_pool.h"

int main(int argc, char** argv) {
    SCOPED_INIT_THREAD_CONTEXT();
    doris::ExecEnv::GetInstance()->init_mem_tracker();
    // Used for unit test
    std::unique_ptr<doris::ThreadPool> non_block_close_thread_pool;

    std::ignore = doris::ThreadPoolBuilder("NonBlockCloseThreadPool")
                          .set_min_threads(12)
                          .set_max_threads(48)
                          .build(&non_block_close_thread_pool);
    doris::ExecEnv::GetInstance()->set_non_block_close_thread_pool(
            std::move(non_block_close_thread_pool));

    doris::thread_context()->thread_mem_tracker_mgr->init();
    std::shared_ptr<doris::MemTrackerLimiter> test_tracker =
            doris::MemTrackerLimiter::create_shared(doris::MemTrackerLimiter::Type::GLOBAL,
                                                    "BE-UT");
    doris::thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(test_tracker);
    doris::ExecEnv::GetInstance()->set_cache_manager(doris::CacheManager::create_global_instance());
    doris::ExecEnv::GetInstance()->set_process_profile(
            doris::ProcessProfile::create_global_instance());
    doris::ExecEnv::GetInstance()->set_storage_page_cache(
            doris::StoragePageCache::create_global_cache(1 << 30, 10, 0));
    doris::ExecEnv::GetInstance()->set_segment_loader(new doris::SegmentLoader(1000, 1000));
    std::string conf = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    auto st = doris::config::init(conf.c_str(), false);
    doris::ExecEnv::GetInstance()->set_tablet_schema_cache(
            doris::TabletSchemaCache::create_global_schema_cache(
                    doris::config::tablet_schema_cache_capacity));
    doris::ExecEnv::GetInstance()->set_delete_bitmap_agg_cache(
            doris::DeleteBitmapAggCache::create_instance(
                    doris::config::delete_bitmap_agg_cache_capacity));
    doris::ExecEnv::GetInstance()->set_tablet_column_object_pool(
            doris::TabletColumnObjectPool::create_global_column_cache(
                    doris::config::tablet_schema_cache_capacity));
    doris::ExecEnv::GetInstance()->set_orc_memory_pool(new doris::vectorized::ORCMemoryPool());

    LOG(INFO) << "init config " << st;
    doris::Status s = doris::config::set_config("enable_stacktrace", "false");
    if (!s.ok()) {
        LOG(WARNING) << "set enable_stacktrace=false failed";
    }

    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();
    doris::BackendOptions::init();

    auto service = std::make_unique<doris::HttpService>(doris::ExecEnv::GetInstance(), 0, 1);
    auto status = service->start();
    if (!s.ok()) {
        LOG(WARNING) << "start http service fail.";
    }

    doris::global_test_http_host = "http://127.0.0.1:" + std::to_string(service->get_real_port());

    ::testing::TestEventListeners& listeners = ::testing::UnitTest::GetInstance()->listeners();
    listeners.Append(new TestListener);
    doris::ExecEnv::set_tracking_memory(false);

    google::ParseCommandLineFlags(&argc, &argv, false);

    updatePHDRCache();
    try {
        int res = RUN_ALL_TESTS();
        doris::ExecEnv::GetInstance()->set_non_block_close_thread_pool(nullptr);
        return res;
    } catch (doris::Exception& e) {
        LOG(FATAL) << "Exception: " << e.what();
    } catch (...) {
        auto eptr = std::current_exception();
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception& e) {
            LOG(FATAL) << "Unknown exception: " << e.what();
        } catch (...) {
            LOG(FATAL) << "Unknown exception";
        }
        return -1;
    }
}
