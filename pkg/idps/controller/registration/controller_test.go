// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package registration

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	tanzucrd "antrea.io/antrea/pkg/apis/tanzucrd/v1alpha1"
	"antrea.io/antrea/pkg/client/clientset/versioned/fake"
	crdinformers "antrea.io/antrea/pkg/client/informers/externalversions"
)

func TestInterworkingController_IsRegistered(t *testing.T) {
	getNewEncryptString := func(str string) string {
		encryptedStr, _ := NewEncryptString(str)
		return string(encryptedStr)
	}
	tests := []struct {
		name             string
		info             *tanzucrd.NSXRegistration
		start            bool
		expectedErrorStr string
	}{
		{
			name:             "Not start",
			info:             nil,
			start:            false,
			expectedErrorStr: "informer is not ready",
		},
		{
			name:             "No info",
			info:             nil,
			start:            true,
			expectedErrorStr: "failed to get the NSXRegistration object",
		},
		{
			name: "Decrypt fail",
			info: &tanzucrd.NSXRegistration{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name: RegistrationInfoName,
				},
				Timestamp: "123456",
			},
			start:            true,
			expectedErrorStr: "failed to decrypt registration timestamp string",
		},
		{
			name: "Invalid timestamp",
			info: &tanzucrd.NSXRegistration{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name: RegistrationInfoName,
				},
				Timestamp: getNewEncryptString("abcd"),
			},
			start:            true,
			expectedErrorStr: "invalid registration timestamp format",
		},
		{
			name: "Success",
			info: &tanzucrd.NSXRegistration{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name: RegistrationInfoName,
				},
				Timestamp: getNewEncryptString(fmt.Sprintf("%d", time.Now().Unix())),
			},
			start: true,
		},
		{
			name: "Expire before",
			info: &tanzucrd.NSXRegistration{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name: RegistrationInfoName,
				},
				Timestamp: getNewEncryptString(fmt.Sprintf("%d", time.Now().Add(registrationExpireTime*10).Unix())),
			},
			start:            true,
			expectedErrorStr: "future registration timestamp",
		},
		{
			name: "Success before",
			info: &tanzucrd.NSXRegistration{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name: RegistrationInfoName,
				},
				Timestamp: getNewEncryptString(fmt.Sprintf("%d", time.Now().Add(registrationExpireTime/2).Unix())),
			},
			start:            true,
			expectedErrorStr: "expired registration",
		},
		{
			name: "Success now",
			info: &tanzucrd.NSXRegistration{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name: RegistrationInfoName,
				},
				Timestamp: getNewEncryptString(fmt.Sprintf("%d", time.Now().Unix())),
			},
			start: true,
		},
		{
			name: "Success after",
			info: &tanzucrd.NSXRegistration{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name: RegistrationInfoName,
				},
				Timestamp: getNewEncryptString(fmt.Sprintf("%d", time.Now().Add(-registrationExpireTime/2).Unix())),
			},
			start: true,
		},
		{
			name: "Expire after",
			info: &tanzucrd.NSXRegistration{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name: RegistrationInfoName,
				},
				Timestamp: getNewEncryptString(fmt.Sprintf("%d", time.Now().Add(-registrationExpireTime*10).Unix())),
			},
			start:            true,
			expectedErrorStr: "expired registration",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fakeCRDClient *fake.Clientset
			if tt.info != nil {
				fakeCRDClient = fake.NewSimpleClientset(tt.info)
			} else {
				fakeCRDClient = fake.NewSimpleClientset()
			}
			crdInformerFactory := crdinformers.NewSharedInformerFactory(fakeCRDClient, 0)
			nsxRegistrationStateInformer := crdInformerFactory.TanzuCrd().V1alpha1().NSXRegistrations()
			m := &NSXRegistrationStateController{
				registrationInformer: nsxRegistrationStateInformer,
				registrationLister:   nsxRegistrationStateInformer.Lister(),
			}
			stopCh := make(chan struct{})
			if tt.start {
				crdInformerFactory.Start(stopCh)
				err := wait.PollImmediate(100*time.Millisecond, time.Second, func() (bool, error) {
					if nsxRegistrationStateInformer.Informer().HasSynced() {
						return true, nil
					}
					return false, nil
				})
				if err != nil {
					t.Errorf("Failed to wait informer synced")
				}
			}
			if err := m.ValidateRegistrationState(); err != nil {
				assert.Contains(t, err.Error(), tt.expectedErrorStr)
			}
			close(stopCh)
		})
	}
}
