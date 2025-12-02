# frozen_string_literal: true

require 'digest'
require 'json'

def normalize(value, options = {})
# Defaults, can be overridden via options
  opts = {
    excluded_types_list: ["InteractionCounter"],
    id_computing_property_list: ["name"],
    unique: true,            # turn array de-dup on/off
    unique_by: ['@id'],      # preferred keys for uniqueness for Hash objects
    flatten_singletons: true # retain your original "flatten arrays of size 1" behavior
  }.merge(options)

  case value
  when Hash
    normalized_hash = normalize_hash(value, opts)

    # If hash contains @value, return that value (normalized to be safe)
    if normalized_hash.key?('@value')
      normalize(normalized_hash['@value'], opts)
    else
      normalized_hash
    end

  when Array
    # Normalize all elements first
    normalized_elems = value.map { |v| normalize(v, opts) }

    # De-duplicate if requested
    normalized_elems = dedup_array(normalized_elems, opts) if opts[:unique]

    # Flatten single-element arrays (preserve your original behavior)
    if opts[:flatten_singletons] && normalized_elems.size == 1
      normalized_elems.first
    else
      normalized_elems
    end
  when String
    value.downcase
  else
    # Primitive types pass through
    value
  end
end

# -- Helpers ---------------------------------------------------------------

def normalize_hash(h, opts)
  hash = h.dup
  hash = hash.transform_keys(&:to_s)

  type = hash['@type']
  id   = hash['@id']

  # If @type exists and @id is missing, compute an @id unless excluded
  if type && (id.nil? || id.to_s.empty?)

    unless opts[:excluded_types_list].include?(type)
      candidates = opts[:id_computing_property_list].map { |p| hash[p] }.compact
      val_to_md5_hash = candidates.first
      if val_to_md5_hash.nil? || val_to_md5_hash.is_a?(Array)
        pp "id_computing_property_list: #{opts[:id_computing_property_list]}"
        pp "keys of input hash #{hash.keys}"
        raise "Error creating val_to_md5_hash for @type=#{type.inspect}"
      end

      hash['@id'] = "#{opts[:record_id]}_#{type.to_s.upcase}_#{Digest::MD5.hexdigest(val_to_md5_hash.to_s)}"
    end
  end

  # Recursively normalize each value
  hash.transform_values { |v| normalize(v, opts) }
end

def dedup_array(arr, opts)
  seen = {}
  arr.each_with_object([]) do |elem, acc|
    key = fingerprint_for(elem, opts)
    next if seen[key]
    seen[key] = true
    acc << elem
  end
end

def fingerprint_for(elem, opts)
  case elem
  when Hash
    # Prefer @id (or any keys defined in unique_by)
    if opts[:unique_by].is_a?(Array)
      uniq_vals = opts[:unique_by].map { |k| elem[k] }.compact
      return "id:#{uniq_vals.join('|')}" unless uniq_vals.empty?
    elsif opts[:unique_by].respond_to?(:call)
      computed = opts[:unique_by].call(elem)
      return "proc:#{computed}" if computed
    end
    # Fallback: deep fingerprint
    "hash:#{deep_fingerprint(elem)}"

  when Array
    "arr:#{deep_fingerprint(elem)}"

  else
    # Primitive: use inspect for stable representation
    "val:#{elem.inspect}"
  end
end

def deep_fingerprint(obj)
  canonical = canonicalize(obj)
  Digest::MD5.hexdigest(JSON.generate(canonical))
end

def canonicalize(obj)
  case obj
  when Hash
    # Sort keys for stable representation
    obj.keys.sort.each_with_object({}) do |k, memo|
      memo[k] = canonicalize(obj[k])
    end
  when Array
    obj.map { |v| canonicalize(v) }
  else
    obj # primitives unchanged
  end
end
