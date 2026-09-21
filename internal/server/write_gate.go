package server

import "fmt"

// pauseCoordinatorWrites drains local coordinated writes and replica applies,
// then rejects new ones until resume.
func (s *GRPCServer) pauseCoordinatorWrites() {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	s.writesPaused = true
	s.replicasPaused = true
}

// resumeCoordinatorWrites allows local writes and replica applies again.
func (s *GRPCServer) resumeCoordinatorWrites() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.replicaApplyMu.Lock()
	defer s.replicaApplyMu.Unlock()

	if s.pendingJoin != nil {
		return fmt.Errorf("resume a join pause with ResumeForJoin")
	}

	s.replicasPaused = false
	s.writesPaused = false
	return nil
}
