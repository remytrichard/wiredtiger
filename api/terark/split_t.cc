
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <string>
#include <vector>
using namespace std;

int main() {
	using namespace boost::algorithm;
	
	//std::string str = "access_pattern_hint=none,allocation_size=4KB,app_metadata=,block_allocation=best,block_compressor=,cache_resident=false,checksum=uncompressed,colgroups=,collator=,columns=,dictionary=0,encryption=(keyid=,name=),exclusive=false,extractor=,format=btree,huffman_key=,huffman_value=,ignore_in_memory_cache_size=false,immutable=false,internal_item_max=0,internal_key_max=0,internal_key_truncate=true,internal_page_max=4KB,key_format=u,key_gap=10,leaf_item_max=0,leaf_key_max=0,leaf_page_max=32KB,leaf_value_max=0,log=(enabled=true),lsm=(auto_throttle=true,bloom=true,bloom_bit_count=16,bloom_config=,bloom_hash_count=8,bloom_oldest=false,chunk_count_limit=0,chunk_max=5GB,chunk_size=10MB,merge_custom=(prefix=,start_generation=0,suffix=),merge_max=15,merge_min=0),memory_page_max=5MB,os_cache_dirty_max=0,os_cache_max=0,prefix_compression=false,prefix_compression_min=4,source=,split_deepen_min_child=0,split_deepen_per_child=0,split_pct=75,type=file,value_format=u,localTempDir=./temp";

	std::string str = "value_format=u,localTempDir=./temp";
	std::vector<std::string> settings;
	split(settings, str, is_any_of(","), token_compress_on);
	// erase blank lines
	//settings.erase(std::remove_if(settings.begin(), settings.end(), [](const std::string& iter) {
	//        return iter.find_first_not_of("\t \n") == std::string::npos;
	//		}));
	cout << "size: " << settings.size() << endl;
	for (auto& str : settings) {
		size_t pos = str.find_first_not_of("\t \n");
		cout << str << "\tpos: " << pos << endl;
	}
	auto it = std::remove_if(settings.begin(), settings.end(), [](const std::string& iter) {
			return iter.find_first_not_of("\t \n") == std::string::npos;
		});
	//cout << "total cnt: " << settings.size() << "\tdistance: " << it - settings.begin() << endl;
	if (it == settings.end()) {
		cout << "at end\n";
	} else {
		cout << "noooot end";
	}
	settings.erase(it);
	for (auto& str : settings) {
		size_t pos = str.find_first_not_of("\t \n");
		cout << str << "\tpos: " << pos << endl;
		}
	return 0;
}
