#include "encoder.h"

#include "isa-l/erasure_code.h"
#include "raft_type.h"

namespace raft {

void Stripe::Filter() {
  // key: The pair of (k, n), it uniquely determines an encoded stripe
  // value: A vector that records the stripe fragments' index
  // std::map<std::pair<int, int>, std::vector<int>> m;
  //
  // for (int i = 0; i < fragments_.size(); ++i) {
  //   const auto& fragment = fragments_[i];
  //   // First checks index and term pair
  //   if (fragment.Index() != this->index_ || fragment.Term() != this->term_) {
  //     continue;
  //   }
  //
  //   if (fragment.Type() == kFragments) {
  //     m[{fragment.GetK(), fragment.GetN()}].push_back(static_cast<int>(i));
  //   }
  //
  //   if (fragment.Type() == kNormal) {
  //     m[{1, 0}].push_back(static_cast<int>(i));
  //   }
  // }
  //
  // // Check if we can decode a full entry:
  // for (const auto& k_stripe : m) {
  //   auto k = k_stripe.first.first;
  //   if (k_stripe.second.size() < k) {
  //     continue;
  //   }
  //   std::vector<LogEntry> after_filter;
  //   for (const auto& i : k_stripe.second) {
  //     after_filter.push_back(fragments_[i]);
  //   }
  //   fragments_ = after_filter;
  //
  //   // Set meta data
  //   SetK(k_stripe.first.first);
  //   SetN(k_stripe.first.second);
  //
  //   assert(after_filter.size() > 0);
  //   SetFragLength(after_filter[0].FragmentSlice().size());
  //   return;
  // }
  //
  // // Here means can not decode a full entry from these collected fragments
  // // Set -1 indicates decode process will directly return false
  // SetK(-1);
  // return;
}

// EncodeSlice should not modify underlying data contained by input slice
bool Encoder::EncodeSlice(const Slice& slice, int k, int m, EncodingResults* results) {
  auto encoding_size = slice.size();

  // NOTE: What if encoding_size is not divisible to k?
  auto fragment_size = (encoding_size + k - 1) / k;
  auto start_ptr = reinterpret_cast<unsigned char*>(slice.data());

  // set input vector
  for (int i = 0; i < k; i++, start_ptr += fragment_size) {
    encode_input_[i] = start_ptr;
  }

  // prepare an ouput vector
  for (int i = 0; i < m; i++) {
    encode_output_[i] = new unsigned char[fragment_size];
  }

  // start encoding process
  auto t = m + k;
  auto g_tbls = new unsigned char[k * m * 32];

  gf_gen_cauchy1_matrix(encode_matrix_, t, k);
  ec_init_tables(k, m, &encode_matrix_[k * k], g_tbls);
  ec_encode_data(fragment_size, k, m, g_tbls, encode_input_, encode_output_);

  // write results: for the first k segments, their data is essentially the encoding
  // input, for the rest m segments, their data is the encoding output
  for (int i = 0; i < k + m; ++i) {
    if (i < k) {
      results->insert(
          {i, Slice(reinterpret_cast<char*>(encode_input_[i]), fragment_size)});
    } else {
      results->insert(
          {i, Slice(reinterpret_cast<char*>(encode_output_[i - k]), fragment_size)});
    }
  }
  return true;
}

bool Encoder::DecodeSlice(const EncodingResults& fragments, int k, int m,
                          Slice* results) {
  assert(results != nullptr);
  assert(k != 0);

  // check if there is at least k fragments in input vector:
  if (fragments.size() < k) {
    return false;
  }

  missing_rows_.clear();
  valid_rows_.clear();

  int n = k + m;

  for (int i = 0; i < k; ++i) {
    missing_rows_.push_back(i);
  }

  // construct missing rows and valid rows vector
  for (const auto& [frag_id, slice] : fragments) {
    if (frag_id < k) {
      missing_rows_.erase(
          std::remove(missing_rows_.begin(), missing_rows_.end(), frag_id));
    }
    valid_rows_.push_back(frag_id);
  }

  std::sort(valid_rows_.begin(), valid_rows_.end());

  auto fragment_size = fragments.begin()->second.size();

  // allocate data for constructing the complete data
  auto complete_length = fragment_size * k;
  char* complete_data = new char[complete_length + 16];
  *results = Slice(complete_data, complete_length);

  // copy fragments data coming from encoding input to complete data
  for (const auto& [frag_id, slice] : fragments) {
    if (frag_id < k) {
      // All fragments have the same size
      assert(slice.size() == fragment_size);
      std::memcpy(complete_data + frag_id * fragment_size, slice.data(), fragment_size);
    }
  }

  // No need to decoding
  if (missing_rows_.size() == 0) {
    return true;
  } else {  // Recover data after decoding
    gf_gen_cauchy1_matrix(encode_matrix_, n, k);

    // Construct the decode matrix
    for (int i = 0; i < k; ++i) {
      auto row = valid_rows_[i];
      for (int j = 0; j < k; ++j) {
        // Copy all valid rows in encode_matrix to errors_matrix
        errors_matrix_[i * k + j] = encode_matrix_[row * k + j];
      }
    }
    // Generate the inverse of errors matrix
    gf_invert_matrix(errors_matrix_, invert_matrix_, k);

    for (decltype(missing_rows_.size()) i = 0; i < missing_rows_.size(); ++i) {
      auto row = missing_rows_[i];
      for (int j = 0; j < k; ++j) {
        encode_matrix_[i * k + j] = invert_matrix_[row * k + j];
      }
    }

    auto g_tbls = new unsigned char[k * missing_rows_.size() * 32];
    ec_init_tables(k, missing_rows_.size(), encode_matrix_, g_tbls);

    // Start doing decoding, set input source address and output destination
    for (decltype(missing_rows_.size()) i = 0; i < missing_rows_.size(); ++i) {
      decode_output_[i] =
          (unsigned char*)(complete_data + missing_rows_[i] * fragment_size);
    }

    auto iter = fragments.begin();
    for (int i = 0; i < k; ++i, ++iter) {
      decode_input_[i] = reinterpret_cast<unsigned char*>(iter->second.data());
    }

    ec_encode_data(fragment_size, k, missing_rows_.size(), g_tbls, decode_input_,
                   decode_output_);
  }
  return true;
}

}  // namespace raft
