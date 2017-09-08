
#ifndef __BRIDGE_H_
#define __BRIDGE_H_

#define DLL_PUBLIC __attribute__ ((visibility ("default")))

#include <vector>

#include "wiredtiger.h"


#if defined(__cplusplus)
extern "C" {
#endif
	//!!! BEFORE any other methods called, always call init first
	DLL_PUBLIC int trk_init();

	// Callback to create a new object.
	DLL_PUBLIC int trk_create(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
				   const char *uri, WT_CONFIG_ARG *config);

	// Callback to initialize a cursor.
	DLL_PUBLIC int trk_open_cursor(WT_DATA_SOURCE *dsrc, WT_SESSION *session,
						const char *uri, WT_CONFIG_ARG *config, WT_CURSOR **new_cursor);

	//Callback performed before an LSM merge.
	DLL_PUBLIC int trk_pre_merge(WT_DATA_SOURCE *dsrc, WT_CURSOR *source, WT_CURSOR *dest);

	DLL_PUBLIC int trk_drop(WT_DATA_SOURCE *dsrc, WT_SESSION *session, const char *uri, WT_CONFIG_ARG *config);
	

	// Return the next record.
	int trk_reader_cursor_next(WT_CURSOR *cursor);

	// Return the previous record.
	int trk_reader_cursor_prev(WT_CURSOR *cursor);

	/*!
	 * Reset the cursor. Any resources held by the cursor are released,
	 * and the cursor's key and position are no longer valid. Subsequent
	 * iterations with WT_CURSOR::next will move to the first record, or
	 * with WT_CURSOR::prev will move to the last record.
	 *
	 * In the case of a statistics cursor, resetting the cursor refreshes
	 * the statistics information returned.
	 */
	int trk_builder_cursor_reset(WT_CURSOR *cursor);
	int trk_reader_cursor_reset(WT_CURSOR *cursor);

	/*!
	 * Return the record matching the key. The key must first be set.
	 *
	 * On success, the cursor ends positioned at the returned record; to
	 * minimize cursor resources, the WT_CURSOR::reset method should be
	 * called as soon as the record has been retrieved and the cursor no
	 * longer needs that position.
	 */
	int trk_reader_cursor_search(WT_CURSOR *cursor);

	/*!
	 * Return the record matching the key if it exists, or an adjacent
	 * record.  An adjacent record is either the smallest record larger
	 * than the key or the largest record smaller than the key (in other
	 * words, a logically adjacent key).
	 *
	 * The key must first be set.
	 *
	 * On success, the cursor ends positioned at the returned record; to
	 * minimize cursor resources, the WT_CURSOR::reset method should be
	 * called as soon as the record has been retrieved and the cursor no
	 * longer needs that position.
	 *
	 * @param exactp the status of the search: 0 if an exact match is
	 * found, < 0 if a smaller key is returned, > 0 if a larger key is
	 * returned
	 */
	int trk_reader_cursor_search_near(WT_CURSOR *cursor, int *exactp);

	/*!
	 * Insert a record and optionally update an existing record.
	 *
	 * If the cursor was configured with "overwrite=true" (the default),
	 * both the key and value must be set; if the record already exists,
	 * the key's value will be updated, otherwise, the record will be
	 * inserted.
	 *
	 * If the cursor was not configured with "overwrite=true", both the key
	 * and value must be set and the record must not already exist; the
	 * record will be inserted.
	 *
	 * If a cursor with record number keys was configured with
	 * "append=true" (not the default), the value must be set; a new record
	 * will be appended and the record number set as the cursor key value.
	 *
	 * The cursor ends with no position, and a subsequent call to the
	 * WT_CURSOR::next (WT_CURSOR::prev) method will iterate from the
	 * beginning (end) of the table.
	 *
	 * If the cursor does not have record number keys or was not configured
	 * with "append=true", the cursor ends with no key set and a subsequent
	 * call to the WT_CURSOR::get_key method will fail. The cursor ends with
	 * no value set and a subsequent call to the WT_CURSOR::get_value method
	 * will fail.
	 *
	 * --------------------------------------------------------------
	 * Inserting a new record after the current maximum record in a
	 * fixed-length bit field column-store (that is, a store with an
	 * 'r' type key and 't' type value) may implicitly create the missing
	 * records as records with a value of 0.
	 *
	 * When loading a large amount of data into a new object, using
	 * a cursor with the \c bulk configuration string enabled and
	 * loading the data in sorted order will be much faster than doing
	 * out-of-order inserts.  See @ref tune_bulk_load for more information.
	 *
	 * The maximum length of a single column stored in a table is not fixed
	 * (as it partially depends on the underlying file configuration), but
	 * is always a small number of bytes less than 4GB.
	 *
	 * In particular, if \c overwrite is not configured and a record with
	 * the specified key already exists, ::WT_DUPLICATE_KEY is returned.
	 * Also, if \c in_memory is configured for the database and the insert
	 * requires more than the configured cache size to complete,
	 * ::WT_CACHE_FULL is returned.
	 * ---------------------------------------------------------------------
	 */
	int trk_builder_cursor_insert(WT_CURSOR *cursor);

	/*!
	 * Close the cursor.
	 *
	 * This releases the resources associated with the cursor handle.
	 * Cursors are closed implicitly by ending the enclosing connection or
	 * closing the session in which they were opened.
	 */
	int trk_builder_cursor_close(WT_CURSOR *cursor);
	int trk_reader_cursor_close(WT_CURSOR *cursor);


#if defined(__cplusplus)
}
#endif


#endif



