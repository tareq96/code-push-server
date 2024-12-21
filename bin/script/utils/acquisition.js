"use strict";
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
Object.defineProperty(exports, "__esModule", { value: true });
exports.getUpdatePackageInfo = void 0;
const semver = require("semver");
const rollout_selector_1 = require("./rollout-selector");
function getUpdatePackageInfo(packageHistory, request) {
    const updatePackage = getUpdatePackage(packageHistory, request, /*ignoreRolloutPackages*/ false);
    let cacheResponse;
    if ((0, rollout_selector_1.isUnfinishedRollout)(updatePackage.rollout)) {
        const origUpdatePackage = getUpdatePackage(packageHistory, request, /*ignoreRolloutPackages*/ true);
        cacheResponse = {
            originalPackage: origUpdatePackage.response,
            rolloutPackage: updatePackage.response,
            rollout: updatePackage.rollout,
        };
    }
    else {
        cacheResponse = { originalPackage: updatePackage.response };
    }
    return cacheResponse;
}
exports.getUpdatePackageInfo = getUpdatePackageInfo;
function getUpdatePackage(packageHistory, request, ignoreRolloutPackages) {
    const updateDetails = {
        downloadURL: "",
        description: "",
        isAvailable: false,
        isMandatory: false,
        appVersion: "",
        packageHash: "",
        label: "",
        packageSize: 0,
        updateAppVersion: false,
    };
    if (!packageHistory || packageHistory.length === 0) {
        updateDetails.shouldRunBinaryVersion = true;
        return { response: updateDetails };
    }
    let foundRequestPackageInHistory = false;
    let latestSatisfyingEnabledPackage;
    let latestEnabledPackage;
    let rollout = null;
    let shouldMakeUpdateMandatory = false;
    for (let i = packageHistory.length - 1; i >= 0; i--) {
        const packageEntry = packageHistory[i];
        // Check if this packageEntry is the same as the one that the client is running.
        // Note that older client plugin versions do not send the release label. If the
        // label is supplied, we use label comparison, since developers can release the
        // same update twice. Otherwise, we fall back to hash comparison.
        // If request is missing both label and hash we take the latest package
        // as we cannot determine which one the client is running
        foundRequestPackageInHistory =
            foundRequestPackageInHistory ||
                (!request.label && !request.packageHash) ||
                (request.label && packageEntry.label === request.label) ||
                (!request.label && packageEntry.packageHash === request.packageHash);
        if (packageEntry.isDisabled || (ignoreRolloutPackages && (0, rollout_selector_1.isUnfinishedRollout)(packageEntry.rollout))) {
            continue;
        }
        latestEnabledPackage = latestEnabledPackage || packageEntry;
        if (!request.isCompanion && !semver.satisfies(request.appVersion, packageEntry.appVersion)) {
            continue;
        }
        latestSatisfyingEnabledPackage = latestSatisfyingEnabledPackage || packageEntry;
        if (foundRequestPackageInHistory) {
            // All the releases further down the history are older than the one the
            // client is running, so we can stop the scan.
            break;
        }
        else if (packageEntry.isMandatory) {
            // If this release is mandatory, newer than the one the client is running,
            // and satifies the client's binary version, we should also make the
            // latest update mandatory. We got all the information we need from the
            // history, so stop the scan.
            shouldMakeUpdateMandatory = true;
            break;
        }
    }
    // If none of the enabled releases have a range that satisfies the client's binary
    // version, tell the client to run the version bundled in the binary.
    updateDetails.shouldRunBinaryVersion = !latestSatisfyingEnabledPackage;
    if (!latestEnabledPackage) {
        // None of the releases in this deployment are enabled, so return no update.
        return { response: updateDetails };
    }
    else if (updateDetails.shouldRunBinaryVersion || latestSatisfyingEnabledPackage.packageHash === request.packageHash) {
        // Either none of the releases in this deployment satisfy the client's binary
        // version, or the client already has the latest relevant update, so return no
        // update, but also tell the client what appVersion the latest release is on and
        // whether they should trigger a store update.
        if (semver.gtr(request.appVersion, latestEnabledPackage.appVersion)) {
            updateDetails.appVersion = latestEnabledPackage.appVersion;
        }
        else if (!semver.satisfies(request.appVersion, latestEnabledPackage.appVersion)) {
            updateDetails.updateAppVersion = true;
            updateDetails.appVersion = latestEnabledPackage.appVersion;
        }
        return { response: updateDetails };
    }
    else if (request.packageHash &&
        latestSatisfyingEnabledPackage.diffPackageMap &&
        latestSatisfyingEnabledPackage.diffPackageMap[request.packageHash]) {
        updateDetails.downloadURL = latestSatisfyingEnabledPackage.diffPackageMap[request.packageHash].url;
        updateDetails.packageSize = latestSatisfyingEnabledPackage.diffPackageMap[request.packageHash].size;
    }
    else {
        updateDetails.downloadURL = latestSatisfyingEnabledPackage.blobUrl;
        updateDetails.packageSize = latestSatisfyingEnabledPackage.size;
    }
    updateDetails.description = latestSatisfyingEnabledPackage.description;
    updateDetails.isMandatory = shouldMakeUpdateMandatory || latestSatisfyingEnabledPackage.isMandatory;
    updateDetails.isAvailable = true;
    updateDetails.label = latestSatisfyingEnabledPackage.label;
    updateDetails.packageHash = latestSatisfyingEnabledPackage.packageHash;
    rollout = latestSatisfyingEnabledPackage.rollout;
    // Old plugins will only work with updates with app versions that are valid semver
    // (i.e. not a range), so we return the same version string as the requested one
    updateDetails.appVersion = request.appVersion;
    return { response: updateDetails, rollout: rollout };
}