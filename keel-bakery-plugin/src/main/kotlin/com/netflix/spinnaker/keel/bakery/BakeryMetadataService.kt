package com.netflix.spinnaker.keel.bakery

import com.netflix.spinnaker.keel.bakery.diff.PackageDiff

/**
 * Provides metadata and related auxiliary functions on baked images.
 */
interface BakeryMetadataService {
  /**
   * @return A map of package names to versions for the specified [imageName].
   */
  suspend fun getPackages(region: String, imageName: String): Map<String, String>

  /**
   * @return A diff of the installed packages between [oldImage] and [newImage].
   * Images must be specified in a format like the following example:
   *  mypackage-0.0.1-h182.905b523-x86_64-20211112233315-bionic-hvm-sriov-ebs
   */
  suspend fun getPackageDiff(region: String, oldImage: String?, newImage: String): PackageDiff
}
