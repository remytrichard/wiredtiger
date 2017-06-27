

#include "rocksdb/options.h"

#include "terark_chunk_manager.h"

namespace rocksdb {

	TerarkChunkManager* TerarkChunkManager::_instance = nullptr;

	TerarkChunkManager* 
	TerarkChunkManager::sharedInstance() {
		if (!_instance) {
			Options opt;
			TerarkZipTableOptions tzo;
			TerarkZipAutoConfigForOnlineDB(tzo, opt, opt);
			int err = 0;
			try {
				TempFileDeleteOnClose test;
				test.path = tzo.localTempDir + "/Terark-XXXXXX";
				test.open_temp();
				test.writer << "Terark";
				test.complete_write();
			} catch (...) {
				fprintf(stderr
						, "ERROR: bad localTempDir %s %s\n"
						, tzo.localTempDir.c_str(), err ? strerror(err) : "");
				abort();
			}
			_instance = new TerarkChunkManager;
			if (tzo.debugLevel < 0) {
				STD_INFO("NewTerarkChunkManager(\n%s)\n",
						 _instance->GetPrintableTableOptions().c_str());
			}
		}
		return _instance;
	}

}
