// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package registration

import (
	"fmt"
	"strconv"
	"time"

	tanzucrdinformers "antrea.io/antrea/pkg/client/informers/externalversions/tanzucrd/v1alpha1"
	tanzucrdlisters "antrea.io/antrea/pkg/client/listers/tanzucrd/v1alpha1"
)

const (
	registrationExpireTime = 10 * time.Minute
	allowedTimeDiff        = time.Hour

	RegistrationInfoName = "interworking-info"

	timeFormat = "2006-01-02 15:04:05"
)

type NSXRegistrationStateController struct {
	registrationInformer tanzucrdinformers.NSXRegistrationInformer
	registrationLister   tanzucrdlisters.NSXRegistrationLister
}

func NewRegistrationController(registrationInformer tanzucrdinformers.NSXRegistrationInformer) *NSXRegistrationStateController {
	return &NSXRegistrationStateController{registrationInformer: registrationInformer, registrationLister: registrationInformer.Lister()}
}

func (m *NSXRegistrationStateController) ValidateRegistrationState() error {
	if !m.registrationInformer.Informer().HasSynced() {
		return fmt.Errorf("informer is not ready")
	}
	registrationInfo, err := m.registrationLister.Get(RegistrationInfoName)
	if err != nil {
		return fmt.Errorf("failed to get the NSXRegistration object: %w", err)
	}
	encryptedTimestamp := EncryptedString(registrationInfo.Timestamp)
	decryptedTimestamp, err := encryptedTimestamp.Decrypt()
	if err != nil {
		return fmt.Errorf("failed to decrypt registration timestamp string: %w", err)
	}
	timestamp, err := strconv.ParseInt(decryptedTimestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid registration timestamp format: %w", err)
	}
	lastRegistrationTimestamp := time.Unix(timestamp, 0)

	nowTimestamp := time.Now()
	if lastRegistrationTimestamp.After(nowTimestamp.Add(allowedTimeDiff)) {
		return fmt.Errorf("future registration timestamp: %s, now timestamp: %s, allowed time diff %s",
			lastRegistrationTimestamp.Format(timeFormat),
			nowTimestamp.Format(timeFormat),
			allowedTimeDiff.String())
	}

	if nowTimestamp.Add(-allowedTimeDiff).After(lastRegistrationTimestamp.Add(registrationExpireTime)) {
		return fmt.Errorf("expired registration timestamp %s, now timestamp: %s, expire time: %s, allowed time diff: %s",
			lastRegistrationTimestamp.Format(timeFormat),
			nowTimestamp.Format(timeFormat),
			registrationExpireTime.String(),
			allowedTimeDiff.String())
	}

	return nil
}
