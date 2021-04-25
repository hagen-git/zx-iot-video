#include <stdio.h>
#include <string.h>

#include "driver/spi_master.h"
#include "esp_err.h"
#include "esp_log.h"
#include "esp_spi_flash.h"
// File System Support
//#include "esp_vfs.h"
#include "esp_spiffs.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"

//#include "zx_server.h"

#include "tape_io.h"
#include "tape_signal.h"

static const char* TAG = "taps";

static void taps_task(void* arg);

#define OVERSAMPLE 1
#define MILLISEC_TO_BYTE_SAMPLES(ms) (OVERSAMPLE * ms * TAPIO_SAMPLE_SPEED_HZ / 1000 / 8)
#define USEC_TO_BYTE_SAMPLES(ms) (OVERSAMPLE * (ms * TAPIO_SAMPLE_SPEED_HZ / 1000) / 1000 / 8)

#define USEC_TO_SAMPLES(ms) (OVERSAMPLE * (ms * TAPIO_SAMPLE_SPEED_HZ / 1000) / 1000)
#define SAMPLES_to_USEC(samples) (samples * 1000 / (OVERSAMPLE * TAPIO_SAMPLE_SPEED_HZ / 1000))

#define BYTE_SAMPLES_to_USEC(bytes) (bytes * 8000 / (OVERSAMPLE * TAPIO_SAMPLE_SPEED_HZ / 1000))

#if 0
static uint8_t outlevel_inv=0;

void stzx_set_out_inv_level(bool inv)
{
	outlevel_inv = inv ? 0xff : 0;
}

static inline void set_sample(uint8_t* samplebuf, uint32_t ix, uint8_t val)
{
    samplebuf[ix]=val;
    //samplebuf[ix]=val ^ outlevel_inv;  // convert for endian byteorder
    
	
	//samplebuf[ix^0x0003]=val ^ outlevel_inv;  // convert for endian byteorder
}
#if OVERSAMPLE > 1
const uint8_t wav_zero[]={  
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00
		};

const uint8_t wav_one[]={
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,

		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,

		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00,
		0x00,0x00,0x00,0x00,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x00,0x00,0x00,0x00

	   };
const uint8_t wav_compr_hdr_std[]={ 0xff,0xff,0xff,0xff,0x00,0x00};
const uint8_t wav_compr_hdr_inv[]={ 0x00,0x00,0x00,0x00,0xff,0xff};
const uint8_t wav_compr_zero[]={ 0xff,0xf8}; // zx81 has iverted input (negative pulse from DC results in 0->1 reading)
const uint8_t wav_compr_one[]={ 0x00,0x07};

#else
const uint8_t wav_zero[]={  0x00,0xff,0xff,0x00,  0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00 };
const uint8_t wav_one[]={   0x00,0xff,0xff,0x00,  0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00, 0x00,0xff,0xff,0x00 };
const uint8_t wav_compr_hdr_std[]={ 0xff,0xf0};
const uint8_t wav_compr_hdr_inv[]={ 0x00,0x0f};
#endif

//const uint8_t wav_compr_hdr[]={ 0xff,0xf0};



typedef struct zxfile_wr_status_info
{
    const uint8_t* wavsample; // pointer to sample, if zero, play
    uint32_t remaining_wavsamples; //
    uint32_t bitcount;
    uint8_t data;
    uint8_t startbit_done; // needed for compressed only
    uint8_t preamble_done; // 
} zxfile_wr_status_t;

static zxfile_wr_status_t zxfile;    // some signal statistics

#define DEBUG_TRANSFERSWITCH 0  // will add some artefacts to observe the gaps between transfers in the oscilloscope

#define IDLE_LEVEL 0x00


// return true if end of file reached
static bool fill_buf_from_file(uint8_t* samplebuf, QueueHandle_t dataq, size_t buffered_filesize, uint8_t file_tag, uint32_t *actual_len_to_fill)
{
	bool end_f=false;
    uint32_t ix=0;

#if DEBUG_TRANSFERSWITCH
	set_sample(samplebuf,ix++,  0xaa );	// mark signals
	for(int d=0;d<30;d++){
		set_sample(samplebuf,ix++,  0xff );
	}
#endif

	if(file_tag==FILE_NOT_ACTIVE){
	    while(ix<MAX_TRANSFER_LEN_BYTES){
			 set_sample(samplebuf,ix++,  IDLE_LEVEL );
		}
		if(zxfile.remaining_wavsamples) zxfile.remaining_wavsamples=zxfile.remaining_wavsamples>MAX_TRANSFER_LEN_BYTES? zxfile.remaining_wavsamples-MAX_TRANSFER_LEN_BYTES :0;
	}else if(file_tag==FILE_TAG_COMPRESSED){
		if(zxfile.bitcount==0 && !zxfile.preamble_done){
			zxfile.remaining_wavsamples=MILLISEC_TO_BYTE_SAMPLES(100); // maybe needed to not react too fast on menu
			zxfile.preamble_done=1;
		}
	    while(ix<MAX_TRANSFER_LEN_BYTES) {
			if(!zxfile.startbit_done && zxfile.remaining_wavsamples==0){
					/* good point to possibly exit here as one full byte is done...*/
					if(ix>MAX_TRANSFER_LEN_BYTES-50*OVERSAMPLE){
						// end of packet will create a 30-60us break at low level, shorter for bigger OVERSAMPLE
						break;
					} 
					zxfile.wavsample=outlevel_inv ? wav_compr_hdr_inv : wav_compr_hdr_std;
					zxfile.remaining_wavsamples=sizeof(wav_compr_hdr_std);
					zxfile.startbit_done=1;
			}
			if(zxfile.remaining_wavsamples){
				set_sample(samplebuf,ix++, zxfile.wavsample ? *zxfile.wavsample++ : IDLE_LEVEL );
				--zxfile.remaining_wavsamples;
			} else {
				zxfile.wavsample=NULL; // after sample, switch back
				if(zxfile.bitcount < buffered_filesize*8 ) {
					if( (zxfile.bitcount&7) ==0){
						if(pdTRUE != xQueueReceive( dataq, &zxfile.data, 0 ) ) ESP_LOGE(TAG, "End of data");
					}

#if OVERSAMPLE > 1
					/* use != operator as logical XOR */
					if(  (0==(zxfile.data &  (0x80 >> (zxfile.bitcount&7) ) ) ) != (outlevel_inv!=0)   ){
						zxfile.wavsample=wav_compr_zero;  // 0 data is high level for std ZX81 
						zxfile.remaining_wavsamples=sizeof(wav_compr_zero);
					}else{
						zxfile.wavsample=wav_compr_one;
						zxfile.remaining_wavsamples=sizeof(wav_compr_one);
					};
					zxfile.bitcount++;
#else
					uint8_t smpl;
					smpl=0;
					if(0==(zxfile.data & (0x80 >> (zxfile.bitcount&7)  ))) smpl|=0xf0;
					zxfile.bitcount++;
					if(0==(zxfile.data & (0x80 >> (zxfile.bitcount&7)  ))) smpl|=0x0f;
					zxfile.bitcount++;
					smpl ^= outlevel_inv ? 0xee : 0x11;
					set_sample(samplebuf,ix++,  smpl );
#endif
					if( (zxfile.bitcount&7) ==0){
						zxfile.startbit_done=0;
					}
				} else {
					set_sample(samplebuf,ix++,  IDLE_LEVEL );
					if(!end_f)	ESP_LOGW(TAG, "End compr file");
					end_f=true;
					break;
				}
			}
		}
	}else{
		/* uncompressed, starts with silence */
		if(zxfile.bitcount==0 && !zxfile.preamble_done){
			zxfile.remaining_wavsamples=MILLISEC_TO_BYTE_SAMPLES(200); // break btw files)
			zxfile.preamble_done=1;
		}
	    while(ix<MAX_TRANSFER_LEN_BYTES) {
			if(zxfile.remaining_wavsamples){
				set_sample(samplebuf,ix++, zxfile.wavsample ? *zxfile.wavsample++ : IDLE_LEVEL );
				--zxfile.remaining_wavsamples;
			} else {
				if (zxfile.wavsample){ // after sample, always insert silence
					zxfile.wavsample=NULL;
					zxfile.bitcount++;  // prepare for next bit
					zxfile.remaining_wavsamples=USEC_TO_BYTE_SAMPLES(1300);
				} else {
					if(zxfile.bitcount < buffered_filesize*8 ) {
						/* good point to exit here as one bit is just done...*/
						if(ix>MAX_TRANSFER_LEN_BYTES-250){
#if DEBUG_TRANSFERSWITCH
							set_sample(samplebuf,ix++,  0xaa ); // mark signals
							for(int d=0;d<50;d++)
								set_sample(samplebuf,ix++,  0xff );
							set_sample(samplebuf,ix++,  0x55 );
#endif
							break;
						}
						if( (zxfile.bitcount&7) ==0){
							if(pdTRUE != xQueueReceive( dataq, &zxfile.data, 0 ) ) ESP_LOGE(TAG, "End of data");
						}
						if(zxfile.data & (0x80 >> (zxfile.bitcount&7)  )){
							zxfile.wavsample=wav_one;
							zxfile.remaining_wavsamples=sizeof(wav_one);
						} else {
							zxfile.wavsample=wav_zero;
							zxfile.remaining_wavsamples=sizeof(wav_zero);
						}
					} else {
						set_sample(samplebuf,ix++,  IDLE_LEVEL );
						if(!end_f)	ESP_LOGW(TAG, "End std file");
						end_f=true;
						break;
					}
				}
			}
        }
    }
	*actual_len_to_fill=ix;
    return end_f;
}


static uint8_t file_busy=0;


// TOSO - Switch to singular call for transfering a file, as 'pipelined' transfer of multiple files is not really needed
#define SEND_HOLDOFF_BYTES 20000  // enough so we do not run out of data on first chunk even when compressed

bool stzx_is_transfer_active()
{
	return file_busy!=0;
}

void stzx_send_cmd(stzx_mode_t cmd, uint8_t data)
{
    i2s_event_t evt;
    static uint8_t file_active=FILE_NOT_ACTIVE;
	static size_t fsize;

	if (cmd==STZX_FILE_START){
		if(file_active)
			ESP_LOGE(TAG, "File double-open");
		if(!file_data_queue){
			file_data_queue=xQueueCreate(16384+512,sizeof(uint8_t));
		}
		if (data!=FILE_TAG_NORMAL && data!=FILE_TAG_COMPRESSED){
			ESP_LOGE(TAG, "Invalid File start mark");
		}

		/* make sur previous file is done and gone, otherwise the byte counting mechanism malfunctios */
		while(file_busy) vTaskDelay(20 / portTICK_RATE_MS);

		file_busy=2;
	    if( xQueueSendToBack( file_data_queue,  &data, 100 / portTICK_RATE_MS ) != pdPASS ) {
	        // Failed to post the message, even after 100 ms.
			ESP_LOGE(TAG, "File write queue blocked");
	    }
		file_active=data;
		fsize=0;
	}
	else if (cmd==STZX_FILE_DATA){
		if(!file_active)
			ESP_LOGE(TAG, "File not open on data write");
	    if( xQueueSendToBack( file_data_queue,  &data, 100 / portTICK_RATE_MS ) != pdPASS )
	    {
	        // Failed to post the message, even after 100 ms.
			ESP_LOGE(TAG, "File write queue blocked");
	    }
	    ++fsize;
    	evt.size=fsize;
	    if(fsize==SEND_HOLDOFF_BYTES){
	    	/* enough bytes in to start off */
	    	evt.type=STZX_FILE_START;
	        if( xQueueSendToBack( event_queue, &evt, 10 / portTICK_RATE_MS ) != pdPASS )	 ESP_LOGE(TAG, "File write event d queue blocked");
	    }
	    else if( fsize%1000==600){
	    	/* provide an update on the buffer level */
	    	evt.type=STZX_FILE_DATA;
	        if( xQueueSendToBack( event_queue, &evt, 10 / portTICK_RATE_MS ) != pdPASS )	 ESP_LOGE(TAG, "File write event d queue blocked");
	    }

	}
	else if (cmd==STZX_FILE_END){
		if(!file_active)
			ESP_LOGE(TAG, "File not open on data write");

		evt.size=fsize;
		if(fsize<SEND_HOLDOFF_BYTES){
			evt.type=STZX_FILE_START;
		    if( xQueueSendToBack( event_queue, &evt, 10 / portTICK_RATE_MS ) != pdPASS )	 ESP_LOGE(TAG, "File write event e queue blocked");
		}
		evt.type=STZX_FILE_END;
		if( xQueueSendToBack( event_queue, &evt, 10 / portTICK_RATE_MS ) != pdPASS )	 ESP_LOGE(TAG, "File write event queue blocked");
		file_active=FILE_NOT_ACTIVE;
	}

}

#endif

static QueueHandle_t tx_cmd_queue;
static QueueHandle_t rx_evt_queue;

static void taps_task(void* arg) {
  //i2s_event_t evt;
  //size_t buffered_file_count=0;

  while (true) {
    /* receive is the default task */
    tapio_clear_transmit_buffers();
    while (uxQueueMessagesWaiting(tx_cmd_queue) == 0) {
      tapio_process_next_transfer(0);
    }
    /* handle transmit */
  }
#if 0

    while(1){
		if(pdTRUE ==  xQueueReceive( event_queue, &evt, 2 ) ) {
			if(evt.type==(i2s_event_type_t)STZX_FILE_START){
				buffered_file_count=evt.size;
                if(pdTRUE != xQueueReceive( file_data_queue, &active_file, 1 ) ) ESP_LOGE(TAG, "File Tag not available");
				ESP_LOGW(TAG, "STZX_FILE_START, inv %x  %d, tag %d", outlevel_inv, buffered_file_count,active_file);
			}
			else if(evt.type==(i2s_event_type_t)STZX_FILE_DATA){
				buffered_file_count=evt.size;
				ESP_LOGW(TAG, "STZX_FILE_DATA, %d",buffered_file_count);
			}
			else if(evt.type==(i2s_event_type_t)STZX_FILE_END){
				buffered_file_count=evt.size;
				ESP_LOGW(TAG, "STZX_FILE_END, %d",buffered_file_count);
			}else{
				ESP_LOGW(TAG, "Unexpected evt %d",evt.type);
			}
		}

		while ( (buffered_file_count && active_file) || zxfile.remaining_wavsamples){
			uint32_t bytes_to_send=0;
			if (fill_buf_from_file(tapio_transmit_buffer[active_transfer_ix],file_data_queue,buffered_file_count,active_file,&bytes_to_send )){
				buffered_file_count=0;
				memset(&zxfile,0,sizeof(zxfile));
				//zxfile.remaining_wavsamples=MILLISEC_TO_BYTE_SAMPLES(400); // break btw files>>> will be done at start
				ESP_LOGD(TAG, "ENDFILE %d",active_file);
				active_file=FILE_NOT_ACTIVE;
				file_busy=1;	// todo only set back after 
			}
			if(bytes_to_send){
				if(num_active_transfers && num_active_transfers>=NUM_PARALLEL_TRANSFERS-1){
					wait_and_finish_transfer(spi);
					ESP_LOGD (TAG, "SEND SPI dwait done");
					num_active_transfers--;
				}
				start_transfer(spi, active_transfer_ix, bytes_to_send*8);
				ESP_LOGD (TAG, "SEND SPI data %x %d  wv%d f%d",active_transfer_ix, bytes_to_send,zxfile.remaining_wavsamples,buffered_file_count);
				num_active_transfers++;
				/* use alternating buffers */
				active_transfer_ix = (active_transfer_ix+1) % NUM_PARALLEL_TRANSFERS;
			}
		}
		if (file_busy==1) file_busy=0;
    }
#endif
}

#define MAX_HEADER_BYTES 0x1B       // including hedaer type and checksum bytes
#define TAPRF_BLOCK_TYPE_HEAD 0x00  // header block type
#define TAPRF_BLOCK_TYPE_DATA 0xFF  // data block type

typedef enum {
  TAPRF_INIT = 0,        /*!< initial status */
  TAPRF_PILOT1_STARTED,  /*!< possibly receiving pilot signal */
  TAPRF_PILOT1_RECEIVED, /*!< pilot detected, wait for header */
  TAPRF_RECEIVE_HEAD,    /*!< receive Jupiter Ace tape header block */
  TAPRF_WAIT_DATA,       /*!< wait for data block pilot */
  TAPRF_PILOT2_STARTED,  /*!< possibly receiving pilot signal */
  TAPRF_PILOT2_RECEIVED, /*!< pilot detected, wait for data */
  TAPRF_RECEIVE_DATA,    /*!< receive Jupiter Ace dictionary or binary data block */
  TAPRF_MAX_STATUS
} taprfs_state_t;

typedef struct tap_rec_file {
  taprfs_state_t state;
  uint8_t bitcount;
  uint8_t cur_byte;
  uint8_t headcount;
  uint8_t head[MAX_HEADER_BYTES];
  uint16_t data_xlen;  //expected data length from header (plus two for block type and checksum)
  uint8_t* data;
  uint16_t pulscount;
  uint16_t bytecount;
  uint16_t namelength;
} tap_rec_file_t;

static tap_rec_file_t recfile;

static void recfile_reset() {
  if (recfile.data) {
    free(recfile.data);
  }
  memset(&recfile, 0, sizeof(recfile));
  recfile.state = TAPRF_INIT;  // PARANOIA
}

static void recfile_bit(uint8_t bitval) {
  recfile.bitcount++;
  recfile.cur_byte <<= 1;
  recfile.cur_byte |= bitval;
  if (recfile.bitcount < 8)
    return;  // not a byte yet, done

  // Process the received byte.

  // ZX81 memory image is preceded by a name that ends with the first inverse char (MSB set).
  // Jupiter Ace has a header block and a data block. TAP format just glues both together.
  // But I (Hagen) don't like the standard TAP format, because it omits the block type bytes (00/FF).

  // log the first (up to 41) received bytes of each 1000-byte block
  if (recfile.bytecount % 1000 <= 40)
    ESP_LOGI(TAG, "recfile byte %d data %02X", recfile.bytecount, recfile.cur_byte);

  if (recfile.state == TAPRF_RECEIVE_HEAD) {
    if (recfile.headcount < MAX_HEADER_BYTES) {
      if (recfile.headcount == 0 && recfile.cur_byte != TAPRF_BLOCK_TYPE_HEAD) {
        ESP_LOGW(TAG, "HEADER block incorrect block type 0x%02x. RESET.\n", recfile.cur_byte);
        recfile_reset();
      }
      recfile.head[recfile.headcount] = recfile.cur_byte;
      recfile.headcount++;
    } else {
      ESP_LOGW(TAG, "HEADER block too long, additional data ingored.\n");
    }
  } else if (recfile.state == TAPRF_RECEIVE_DATA) {
    if (!recfile.data) {
      ESP_LOGW(TAG, "RESET because data block not allocated.\n");
      recfile_reset();
      return;
    }
    if (recfile.bytecount < recfile.data_xlen) {
      if (recfile.bytecount == 0 && recfile.cur_byte != TAPRF_BLOCK_TYPE_DATA) {
        ESP_LOGW(TAG, "DATA block incorrect block type 0x%02x. RESET.\n", recfile.cur_byte);
        recfile_reset();
        return;
      }
      recfile.data[recfile.bytecount] = recfile.cur_byte;
    } else {
      ESP_LOGW(TAG, "DATA block too long, additional data ignored.\n");
    }
  }
  recfile.bytecount++;

  //   if (recfile.namelength==0 && (recfile.data&0x80) ) recfile.namelength=recfile.bytecount;
  recfile.bitcount = 0;
  recfile.cur_byte = 0;
}

/* Jupiter Ace Header format

	Ofs	Len	Description
	0	1	block type 0x00 for header
	1	1	file type: 0x00 for Dictionary, 0x20 for bytes file (anything non-zero is fine)
	2	10	filename (ASCII), padded with spaces (0x20)
	12	2	length of file
	14	2	start address, a Dictionary starts at 15441/0x3C51
	16	2	current word. Unused for bytes file, i.e. 8224/0x2020
	18	2	value of system var CURRENT [Address 15409/0x3C31]. Unused* for bytes file
	20	2	value of system var CONTEXT [Address 15411/0x3C33]. Unused* for bytes file
	22	2	value of system var VOCLNK  [Address 15413/0x3C35]. Unused* for bytes file
	24	2	value of system var STKBOT  [Address 15415/0x3C37]. Unused* for bytes file
	26	1	XOR checksum of all bytes from 1 to 25

	Total length: 27/0x1B bytes including block type and checksum
*/
static void recfile_proc_header() {
  ESP_LOGI(TAG, "HEADER processing %d bytes, %d bits", recfile.headcount, recfile.bitcount);
  if (recfile.namelength) {
    ESP_LOGW(TAG, "HEADER processing duplicate call");
    return;
  }
  {  // Compute name length
    int cnt = 0;
    while (cnt < 10 && recfile.head[2 + cnt] > 0x20)
      cnt++;
    recfile.namelength = cnt;
    if (!cnt) {
      ESP_LOGW(TAG, "HEADER processing: empty file name.");
    }
  }
  // Compute expected data length (inclusive block type and checksum)
  recfile.data_xlen = 2 + (recfile.head[12] | (recfile.head[13] << 8));
  // Allocate memory for data file
  recfile.data = (uint8_t*)malloc(recfile.data_xlen);
  if (recfile.data) {
    recfile.state = TAPRF_WAIT_DATA;
    recfile.bytecount = 0;  // reset byte count for data
    return;
  }
  ESP_LOGW(TAG, "HEADER processing: malloc for %d bytes failed. RESET.", recfile.data_xlen);
  recfile_reset();
}

#define SPIFFS_ACE_FILENAME_MAX 50
static void recfile_finalize() {
  ESP_LOGI(TAG, "DATA processing %d bytes, %d bits", recfile.headcount, recfile.bitcount);
  if (!recfile.namelength) {
    ESP_LOGW(TAG, "DATA processing duplicate call?");
    return;
  }
  FILE* fd = NULL;
  char filepath[SPIFFS_ACE_FILENAME_MAX];
  const char fp_prefix[] = { '/', 's', 'p', 'i', 'f', 'f', 's', '/'};
  const char ext[] = {'.', 't', 'z', 'x', (char)0};
  const char tzx_header[] = {'Z', 'X', 'T', 'a', 'p', 'e', '!', (char)0x1A, (char)0x01, (char)0x0d};
  const char txt_wespi[] = {'R', 'e', 'c', ':', 'Z', 'X', '-', 'W', 'e', 's', 'p', 'i',
                            '-', 'V', '-', 'A', 'c', 'e'};
  // Make a Spiffs and TZX file name
  int pos = 0;
  int len = sizeof(fp_prefix);
  memcpy(&filepath[pos], &fp_prefix, len);
  pos += len;
  len = recfile.namelength;
  memcpy(&filepath[pos], &recfile.head[2], len);
  pos += len;
  len = sizeof(ext);
  memcpy(&filepath[pos], &ext, len);
  ESP_LOGI(TAG, "recfile finalize '%s', HEAD %d bytes, DATA %d bytes", filepath, recfile.headcount, recfile.bytecount);
  // Save header and data blocks as TZX file in mass storage
  fd = fopen(filepath, "w");
  if (fd) {
    fwrite(tzx_header, sizeof(tzx_header), 1, fd);
    fputc(0x10, fd);                                 // TZX Standard Speed Data Block
    fputc(0xE8, fd);                                 // 1000ms (0x03E8) pause after header block
    fputc(0x03, fd);                                 // pause high byte
    fputc(recfile.headcount & 0xFF, fd);             // length low byte
    fputc((recfile.headcount >> 8) & 0xFF, fd);      // length high byte
    fwrite(recfile.head, recfile.headcount, 1, fd);  // tape header, including block type and checksum
    fputc(0x10, fd);                                 // TZX Standard Speed Data Block
    fputc(0xB8, fd);                                 // 3000ms (0x0BB8) pause after data block
    fputc(0x0B, fd);                                 // pause high byte
    fputc(recfile.data_xlen & 0xFF, fd);             // length low byte
    fputc((recfile.data_xlen >> 8) & 0xFF, fd);      // length high byte
    fwrite(recfile.data, recfile.data_xlen, 1, fd);  // tape data, including block type and checksum
    fputc(0x30, fd);                                 // TZX Text description
    fputc(sizeof(txt_wespi), fd);                    // length of block
    fwrite(txt_wespi, sizeof(txt_wespi), 1, fd);     // text description
    fclose(fd);
  } else {
    ESP_LOGE(TAG, "Failed to create file : %s", filepath);
  }
  // Ready to receive next file
  recfile_reset();
}

static void rec_pilot_pulse(uint32_t duration) {
  switch (recfile.state) {
    case TAPRF_INIT: {
      recfile.pulscount = 0;
      recfile.state = TAPRF_PILOT1_STARTED;
    } break;
    case TAPRF_WAIT_DATA: {
      recfile.pulscount = 0;
      recfile.state = TAPRF_PILOT2_STARTED;
    } break;
    case TAPRF_PILOT1_STARTED: {
      recfile.pulscount++;
      if (recfile.pulscount == 256) { /* Jupiter Ace ROM requires 256 pulses, choose about the same here */
        ESP_LOGW(TAG, "TAPRF_PILOT1_RECEIVED\n");
        recfile.state = TAPRF_PILOT1_RECEIVED;
      }
    } break;
    case TAPRF_PILOT2_STARTED: {
      recfile.pulscount++;
      if (recfile.pulscount == 256) { /* Jupiter Ace ROM requires 256 pulses, choose about the same here */
        ESP_LOGW(TAG, "TAPRF_PILOT2_RECEIVED\n");
        recfile.state = TAPRF_PILOT2_RECEIVED;
      }
    } break;
    case TAPRF_PILOT1_RECEIVED: {
      recfile.pulscount++;  // waiting for header
    } break;
    case TAPRF_PILOT2_RECEIVED: {
      recfile.pulscount++;  // waiting for data
    } break;
    case TAPRF_RECEIVE_HEAD: {
      ESP_LOGI(TAG, "Follow-up impulse after HEAD %d %d ", recfile.bitcount, recfile.bytecount);
      recfile_proc_header();
    } break;
    case TAPRF_RECEIVE_DATA: {
      ESP_LOGI(TAG, "Follow-up impulse after DATA %d %d ", recfile.bitcount, recfile.bytecount);
      recfile_finalize();
    } break;
    default: {
      ESP_LOGW(TAG, "RESET: unknown pulse type / status\n");
      recfile_reset();
    }
  }
}

static void rec_0_pulse() {
  // The SYNC pulse is about as long as a '0' bit.
  switch (recfile.state) {
    case TAPRF_PILOT1_RECEIVED: {
      ESP_LOGW(TAG, "TAPRF_RECEIVE_HEAD\n");
      recfile.state = TAPRF_RECEIVE_HEAD;
      recfile.bytecount = 0;
      recfile.bitcount = 0;
      recfile.cur_byte = 0;
    } break;
    case TAPRF_PILOT2_RECEIVED: {
      ESP_LOGW(TAG, "TAPRF_RECEIVE_DATA\n");
      recfile.state = TAPRF_RECEIVE_DATA;
      recfile.bytecount = 0;
      recfile.bitcount = 0;
      recfile.cur_byte = 0;
    } break;
    case TAPRF_RECEIVE_HEAD: {
      recfile_bit(0);
    } break;
    case TAPRF_RECEIVE_DATA: {
      recfile_bit(0);
    } break;
    default: {
      ESP_LOGW(TAG, "RESET after unexpected 0 pulse\n");
      recfile_reset();
    }
  }
}

static void rec_1_pulse(uint32_t duration) {
  switch (recfile.state) {
    case TAPRF_PILOT1_STARTED:
    case TAPRF_PILOT1_RECEIVED:
    case TAPRF_PILOT2_STARTED:
    case TAPRF_PILOT2_RECEIVED: {
      ESP_LOGW(TAG, "rec_1_pulse during PILOT %d us", SAMPLES_to_USEC(duration));
    } break;
    case TAPRF_RECEIVE_HEAD: {
      recfile_bit(1);
    } break;
    case TAPRF_RECEIVE_DATA: {
      recfile_bit(1);
    } break;
    default:
      ESP_LOGW(TAG, "RESET after unexpected 1 pulse\n");
      recfile_reset();
  }
}

static void rec_noise_pulse() {
  if (recfile.state != TAPRF_INIT && recfile.state != TAPRF_WAIT_DATA) {
    ESP_LOGW(TAG, "RESET after unexpected noise pulse\n");
    recfile_reset();
  }
}

static bool current_logic_level = false;
static uint32_t level_cnt = 0;
static uint32_t pulse_cnt = 0;

static void analyze_1_to_0(uint32_t duration) {
  // end of high phase,  245 for 0 , 488 for 1 , or 618us for pilot, 277 for end mark, 1.288 for gap
  if (duration < USEC_TO_SAMPLES(150))
    rec_noise_pulse();
  else if (duration <= USEC_TO_SAMPLES(350))
    rec_0_pulse();
  else if (duration <= USEC_TO_SAMPLES(550))
    rec_1_pulse(duration);
  else if (duration <= USEC_TO_SAMPLES(680))
    rec_pilot_pulse(duration);
  // else might be pause

  if (duration < 2 || duration > 500 || (pulse_cnt & 0x1ff) < 5)
    ESP_LOGW(TAG, "High pulse %d smpls, %d us", duration, SAMPLES_to_USEC(duration));

  pulse_cnt++;
}

static void check_on_const_level() {
  if (level_cnt < USEC_TO_SAMPLES(3000))
    return;
  if (recfile.state == TAPRF_INIT)
    return;
  if (recfile.state == TAPRF_WAIT_DATA)
    return;
  if (recfile.state == TAPRF_RECEIVE_HEAD) {
    recfile_proc_header();
  } else if (recfile.state == TAPRF_RECEIVE_DATA) {
    recfile_finalize();
  } else {
    ESP_LOGW(TAG, "RESET after unexpected silence\n");
    recfile_reset();
  }
}

static void analyze_0_to_1(uint32_t duration) {
  if (duration > 1000)
    ESP_LOGW(TAG, "High after long low - %d smpls, %d us", duration, SAMPLES_to_USEC(duration));
}

int __builtin_clz(unsigned int x);
int __builtin_ctz(unsigned int x);  // trailing zeros

/* every incoming 8-bit sample MSB first */
void IRAM_ATTR rx_checksample(uint8_t data) {
  if (data == 0) {
    if (current_logic_level) {
      analyze_1_to_0(level_cnt);
      current_logic_level = false;
      level_cnt = 8;
    } else {
      level_cnt += 8;
    }
  } else if (data == 0xff) {
    if (!current_logic_level) {
      analyze_0_to_1(level_cnt);
      current_logic_level = true;
      level_cnt = 8;
    } else {
      level_cnt += 8;
    }
  } else {
    // level change within byte, assume just one transition
    int cnt_newlvl = __builtin_ctz(data ^ (current_logic_level ? 0 : 0xff));
    level_cnt += 8 - cnt_newlvl;
    if (current_logic_level)
      analyze_1_to_0(level_cnt);
    else
      analyze_0_to_1(level_cnt);
    current_logic_level = !current_logic_level;
    level_cnt = cnt_newlvl;
  }
}

static void on_rx_data(uint8_t* data, int size_bits) {
  static uint32_t acc_kbytes = 0;
  ESP_LOGD(TAG, "on_rx_data %d bits", size_bits);
  acc_kbytes += size_bits / 8192;
  if ((acc_kbytes & 0xff) == 0)
    ESP_LOGW(TAG, "on_rx_data acc %d Mbytes %x, plscnt=%d ", acc_kbytes / 1024, data[0], pulse_cnt);
  // check for all-0 here as this is usually the case
  if (level_cnt > USEC_TO_SAMPLES(2500) && !current_logic_level) {
    for (int i = 0; i < size_bits / 8; i++) {
      if (data[i]) goto not_just_all_0;
    }
    // simply have all-0
    level_cnt += size_bits;
    check_on_const_level();
  } else {
  not_just_all_0:

    for (int i = 0; i < size_bits / 8; i++) {
      rx_checksample(data[i]);
    }
  }
}

void taps_rx_set_queue_to_use(QueueHandle_t rx_evt_q) {
  rx_evt_queue = rx_evt_q;
}

// call once at startup
void taps_init() {
  tx_cmd_queue = xQueueCreate(5, sizeof(taps_tx_packet_t));
  tapio_init(on_rx_data);
  xTaskCreate(taps_task, "taps_task", 1024 * 3, NULL, 9, NULL);
}
